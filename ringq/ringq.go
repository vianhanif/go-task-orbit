package ringq

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/vianhanif/go-task-orbit/executor"
	"github.com/vianhanif/go-task-orbit/internal/timerwheel"
	"github.com/vianhanif/go-task-orbit/ring"
)

var (
	ErrNoTransport = errors.New("ringq: no transport configured")
	ErrNoHandlers  = errors.New("ringq: no handlers registered")
)

type handlerEntry struct {
	fn          DispatchFunc
	maxRetries  int
	baseDelay   time.Duration
	coordinator RetryCoordinator
}

type runtime struct {
	ringBuf    *ring.Buffer
	pool       *executor.Pool
	handlers   map[string]handlerEntry
	transport  Transport
	hooks      Hooks
	idemFilter *idemFilter
	timerWheel *timerwheel.Wheel
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
	return p.HandleWithRetry(topic, fn, 10, 1*time.Second, nil)
}

func (p *Pipeline) HandleWithRetry(topic string, fn DispatchFunc, maxRetries int, baseDelay time.Duration, coordinator RetryCoordinator) *Pipeline {
	p.mu.Lock()
	p.handlers[topic] = handlerEntry{
		fn:          fn,
		maxRetries:  maxRetries,
		baseDelay:   baseDelay,
		coordinator: coordinator,
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

	tw := timerwheel.New()

	rt := &runtime{
		ringBuf:    ringBuf,
		pool:       pool,
		handlers:   handlers,
		transport:  transport,
		hooks:      hooks,
		idemFilter: idemFilter,
		timerWheel: tw,
	}

	timerCtx, timerCancel := context.WithCancel(ctx)
	var timerWg sync.WaitGroup
	timerWg.Add(1)
	go func() {
		defer timerWg.Done()
		rt.runTimer(timerCtx, tw.Start(timerCtx))
	}()

	pollCtx, pollCancel := context.WithCancel(ctx)

	var pollerWg sync.WaitGroup
	pollerWg.Add(1)
	go func() {
		defer pollerWg.Done()
		if err := rt.runPoller(pollCtx); err != nil {
			pollCancel()
		}
	}()

	dispCtx, dispCancel := context.WithCancel(ctx)
	var dispWg sync.WaitGroup
	dispWg.Add(1)
	go func() {
		defer dispWg.Done()
		rt.runDispatcher(dispCtx)
	}()

	select {
	case <-ctx.Done():
	case <-pollCtx.Done():
	}

	dispCancel()
	dispWg.Wait()

	timerCancel()
	timerWg.Wait()

	transport.Close()
	pollerWg.Wait()

	pool.StopDispatching()
	pool.Wait()

	ringBuf.Close()

	for _, item := range tw.Drain() {
		if msg, ok := item.(Message); ok {
			ringBuf.Enqueue(msg)
		}
	}

	if idemFilter != nil {
		idemFilter.close()
	}

	return ctx.Err()
}

func (r *runtime) runPoller(ctx context.Context) error {
	return r.transport.Subscribe(ctx, func(subCtx context.Context, msgs []Message) error {
		if r.hooks.OnReceive != nil {
			r.hooks.OnReceive(subCtx, len(msgs))
		}
		for _, m := range msgs {
			if r.hooks.OnDispatch != nil {
				r.hooks.OnDispatch(subCtx, m.Topic)
			}
			if m.NotBefore > 0 {
				r.timerWheel.Insert(m, m.NotBefore)
			} else {
				r.ringBuf.Enqueue(m)
			}
		}
		return nil
	})
}

func (r *runtime) runTimer(ctx context.Context, ch <-chan []interface{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case batch, ok := <-ch:
			if !ok {
				return
			}
			for _, item := range batch {
				msg, ok := item.(Message)
				if !ok {
					continue
				}
				r.ringBuf.Enqueue(msg)
			}
		}
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
		filtered, duplicates := r.idemFilter.filterDuplicates(ctx, messages)
		messages = filtered
		for _, dup := range duplicates {
			r.transport.Ack(ctx, []Message{dup})
		}
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

	outcome := r.resolveOutcome(ctx, msg, entry, result)

	switch outcome.Action {
	case Ack:
		if err := r.transport.Ack(ctx, []Message{outcome.Message}); err != nil {
			r.hooks.fireOnError(ctx, msg.Topic, err)
		}
		if r.idemFilter != nil {
			r.idemFilter.mark(ctx, outcome.Message)
		}

	case Retry:
		r.hooks.fireOnRetry(ctx, msg.Topic, outcome.Message, outcome.Message.Attempts)
		r.ringBuf.Enqueue(outcome.Message)

	case RetryWithDelay:
		r.hooks.fireOnRetry(ctx, msg.Topic, outcome.Message, outcome.Message.Attempts)
		r.transport.Nack(ctx, outcome.Message, outcome.Delay)

	case DLQ:
		r.hooks.fireOnError(ctx, msg.Topic, outcome.Err)
		if dlqErr := r.transport.SendToDLQ(ctx, outcome.Message); dlqErr != nil {
			r.hooks.fireOnError(ctx, msg.Topic, fmt.Errorf("dlq: send failed: %w", dlqErr))
			return
		}
		r.transport.Ack(ctx, []Message{outcome.Message})
	}
}

func (r *runtime) resolveOutcome(ctx context.Context, msg Message, entry handlerEntry, result Result) RetryOutcome {
	if entry.coordinator != nil {
		return entry.coordinator.Handle(ctx, msg, result)
	}
	return defaultRetryCoordinator{maxRetries: entry.maxRetries, baseDelay: entry.baseDelay}.Handle(ctx, msg, result)
}

type defaultRetryCoordinator struct {
	maxRetries int
	baseDelay  time.Duration
}

const maxRetryDelay = 5 * time.Minute

func (d defaultRetryCoordinator) Handle(_ context.Context, msg Message, result Result) RetryOutcome {
	switch result.Action {
	case Ack:
		return RetryOutcome{Action: Ack, Message: msg}

	case Retry, RetryWithDelay:
		attempt := msg.Attempts + 1
		if attempt > d.maxRetries {
			return RetryOutcome{
				Action:  DLQ,
				Message: msg,
				Err:     fmt.Errorf("retry: max retries (%d) exceeded: %w", d.maxRetries, result.Err),
			}
		}
		msg.Attempts = attempt
		action := result.Action
		delay := result.Delay
		if delay <= 0 {
			delay = d.baseDelay * time.Duration(int64(math.Pow(2, float64(attempt-1))))
			if delay > maxRetryDelay {
				delay = maxRetryDelay
			}
		}
		if delay > 0 {
			action = RetryWithDelay
		}
		return RetryOutcome{Action: action, Message: msg, Delay: delay}

	case DLQ:
		return RetryOutcome{Action: DLQ, Message: msg, Err: result.Err}

	default:
		return RetryOutcome{Action: DLQ, Message: msg, Err: fmt.Errorf("retry: unknown action %v", result.Action)}
	}
}

func (r *runtime) handleUnknownTopic(ctx context.Context, msg Message) {
	err := fmt.Errorf("ringq: no handler for topic %q", msg.Topic)
	r.hooks.fireOnError(ctx, msg.Topic, err)

	if dlqErr := r.transport.SendToDLQ(ctx, msg); dlqErr != nil {
		r.hooks.fireOnError(ctx, msg.Topic, fmt.Errorf("dlq: send failed: %w", dlqErr))
		return
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

func (h Hooks) fireOnRetry(ctx context.Context, topic string, msg Message, attempt int) {
	if h.OnRetry != nil {
		h.OnRetry(ctx, topic, msg, attempt)
	}
}
