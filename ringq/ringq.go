package ringq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/vianhanif/go-task-orbit/executor"
	"github.com/vianhanif/go-task-orbit/ring"
)

var (
	ErrNoTransport = errors.New("ringq: no transport configured")
	ErrNoHandlers  = errors.New("ringq: no handlers registered")
)

type handlerEntry struct {
	fn        DispatchFunc
	maxRetries int
	baseDelay  time.Duration
}

type runtime struct {
	ringBuf    *ring.Buffer
	pool       *executor.Pool
	handlers   map[string]handlerEntry
	transport  Transport
	hooks      Hooks
	idemFilter *idemFilter
}

type Pipeline struct {
	mu        sync.Mutex
	transport Transport
	handlers  map[string]handlerEntry
	hooks     Hooks
	idemConf  *IdempotencyConfig
	config    Config
}

func New() *Pipeline {
	return &Pipeline{
		handlers: make(map[string]handlerEntry),
		config: Config{
			Concurrency: 10,
			BufferSize:  1024,
		},
	}
}

func (p *Pipeline) Transport(t Transport) *Pipeline {
	p.mu.Lock()
	p.transport = t
	p.mu.Unlock()
	return p
}

func (p *Pipeline) Handle(topic string, fn DispatchFunc) *Pipeline {
	return p.HandleWithRetry(topic, fn, 3, 5*time.Second)
}

func (p *Pipeline) HandleWithRetry(topic string, fn DispatchFunc, maxRetries int, baseDelay time.Duration) *Pipeline {
	p.mu.Lock()
	p.handlers[topic] = handlerEntry{
		fn:         fn,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
	}
	p.mu.Unlock()
	return p
}

func (p *Pipeline) Idempotency(cfg IdempotencyConfig) *Pipeline {
	p.mu.Lock()
	p.idemConf = &cfg
	p.mu.Unlock()
	return p
}

func (p *Pipeline) WithHooks(h Hooks) *Pipeline {
	p.mu.Lock()
	p.hooks = h
	p.mu.Unlock()
	return p
}

func (p *Pipeline) Concurrency(n int) *Pipeline {
	p.mu.Lock()
	p.config.Concurrency = n
	p.mu.Unlock()
	return p
}

func (p *Pipeline) BufferSize(n int) *Pipeline {
	p.mu.Lock()
	p.config.BufferSize = n
	p.mu.Unlock()
	return p
}

func (p *Pipeline) Run(ctx context.Context) error {
	p.mu.Lock()
	transport := p.transport
	handlers := p.handlers
	hooks := p.hooks
	idemConf := p.idemConf
	cfg := p.config
	p.mu.Unlock()

	if transport == nil {
		return ErrNoTransport
	}
	if len(handlers) == 0 {
		return ErrNoHandlers
	}

	ringBuf := ring.New(cfg.BufferSize, ring.Block)

	pool := executor.New(cfg.Concurrency)
	pool.Start()

	var idemFilter *idemFilter
	if idemConf != nil {
		idemFilter = newIdemFilter(*idemConf, hooks.OnDuplicate)
	}

	rt := &runtime{
		ringBuf:   ringBuf,
		pool:      pool,
		handlers:  handlers,
		transport: transport,
		hooks:     hooks,
		idemFilter: idemFilter,
	}

	var pollerWg sync.WaitGroup
	pollerWg.Add(1)
	go func() {
		defer pollerWg.Done()
		rt.runPoller(ctx)
	}()

	dispCtx, dispCancel := context.WithCancel(ctx)
	var dispWg sync.WaitGroup
	dispWg.Add(1)
	go func() {
		defer dispWg.Done()
		rt.runDispatcher(dispCtx)
	}()

	<-ctx.Done()

	dispCancel()
	dispWg.Wait()

	transport.Close()
	pollerWg.Wait()

	pool.StopDispatching()
	pool.Wait()

	ringBuf.Close()

	if idemFilter != nil {
		idemFilter.close()
	}

	return ctx.Err()
}

func (r *runtime) runPoller(ctx context.Context) {
	err := r.transport.Subscribe(ctx, func(subCtx context.Context, msgs []Message) error {
		if r.hooks.OnReceive != nil {
			r.hooks.OnReceive(subCtx, len(msgs))
		}
		if r.hooks.OnDispatch != nil {
			for _, m := range msgs {
				r.hooks.OnDispatch(subCtx, m.Topic)
			}
		}
		for _, m := range msgs {
			r.ringBuf.Enqueue(m)
		}
		return nil
	})
	if err != nil && err != context.Canceled {
		log.Printf("ringq: transport subscribe error: %v", err)
	}
}

func (r *runtime) runDispatcher(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			for {
				batch := r.ringBuf.DequeueBatch(100)
				if len(batch) == 0 {
					return
				}
				r.dispatchBatch(ctx, batch)
			}
		default:
		}

		batch := r.ringBuf.DequeueBatch(10)
		if len(batch) == 0 {
			select {
			case <-ctx.Done():
				return
			default:
			}
			continue
		}
		r.dispatchBatch(ctx, batch)
	}
}

func (r *runtime) dispatchBatch(ctx context.Context, batch []interface{}) {
	messages := make([]Message, 0, len(batch))
	for _, item := range batch {
		msg, ok := item.(Message)
		if !ok {
			continue
		}
		messages = append(messages, msg)
	}

	if r.idemFilter != nil {
		messages = r.idemFilter.filter(ctx, messages)
	}

	var wg sync.WaitGroup
	for _, msg := range messages {
		msg := msg
		entry, ok := r.handlers[msg.Topic]
		if !ok {
			wg.Add(1)
			r.pool.Submit(func() {
				defer wg.Done()
				r.handleUnknownTopic(ctx, msg)
			})
			continue
		}

		wg.Add(1)
		r.pool.Submit(func() {
			defer wg.Done()
			r.processMessage(ctx, msg, entry)
		})
	}
	wg.Wait()
}

func (r *runtime) processMessage(ctx context.Context, msg Message, entry handlerEntry) {
	start := time.Now()

	result := entry.fn(ctx, msg.Payload)

	r.hooks.fireOnComplete(ctx, msg.Topic, time.Since(start))

	switch result.Action {
	case Ack:
		if err := r.transport.Ack(ctx, []Message{msg}); err != nil {
			r.hooks.fireOnError(ctx, msg.Topic, err)
		}
		if r.idemFilter != nil {
			r.idemFilter.mark(ctx, msg)
		}

	case Retry:
		r.handleRetry(ctx, msg, entry, result)

	case RetryWithDelay:
		r.handleRetry(ctx, msg, entry, result)

	case DLQ:
		r.sendToDLQ(ctx, msg, result.Err)
	}
}

func (r *runtime) handleRetry(ctx context.Context, msg Message, entry handlerEntry, result Result) {
	attempt := msg.Attempts + 1
	if attempt > entry.maxRetries {
		err := fmt.Errorf("retry: max retries (%d) exceeded: %w", entry.maxRetries, result.Err)
		r.hooks.fireOnError(ctx, msg.Topic, err)
		r.sendToDLQ(ctx, msg, err)
		return
	}

	msg.Attempts = attempt
	r.hooks.fireOnRetry(ctx, msg.Topic, attempt)

	switch result.Action {
	case Retry:
		r.ringBuf.Enqueue(msg)
	case RetryWithDelay:
		delay := result.Delay
		if delay <= 0 {
			delay = entry.baseDelay
		}
		r.transport.Nack(ctx, msg, delay)
	}
}

func (r *runtime) handleUnknownTopic(ctx context.Context, msg Message) {
	r.hooks.fireOnError(ctx, msg.Topic, fmt.Errorf("ringq: no handler for topic %q", msg.Topic))
	r.transport.SendToDLQ(ctx, msg)
	r.transport.Ack(ctx, []Message{msg})
}

func (r *runtime) sendToDLQ(ctx context.Context, msg Message, err error) {
	if err != nil {
		r.hooks.fireOnError(ctx, msg.Topic, err)
	}
	if dlqErr := r.transport.SendToDLQ(ctx, msg); dlqErr != nil {
		r.hooks.fireOnError(ctx, msg.Topic, fmt.Errorf("dlq: send failed: %w", dlqErr))
	}
	r.transport.Ack(ctx, []Message{msg})
}

func (h Hooks) fireOnComplete(ctx context.Context, topic string, dur time.Duration) {
	if h.OnComplete != nil {
		h.OnComplete(ctx, topic, dur)
	}
}

func (h Hooks) fireOnError(ctx context.Context, topic string, err error) {
	if h.OnError != nil {
		h.OnError(ctx, topic, err)
	}
}

func (h Hooks) fireOnRetry(ctx context.Context, topic string, attempt int) {
	if h.OnRetry != nil {
		h.OnRetry(ctx, topic, attempt)
	}
}
