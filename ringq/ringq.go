package ringq

import (
	"context"
	"errors"
	"sync"
)

var (
	ErrNoTransport = errors.New("ringq: no transport configured")
	ErrNoHandlers  = errors.New("ringq: no handlers registered")
)

type Pipeline struct {
	mu       sync.Mutex
	transport Transport
	handlers map[string]interface{}
	hooks    Hooks
	idemConf *IdempotencyConfig
	config   Config
	codec    Codec[[]byte]
}

func New() *Pipeline {
	return &Pipeline{
		handlers: make(map[string]interface{}),
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

func (p *Pipeline) Handle(topic string, handler interface{}) *Pipeline {
	p.mu.Lock()
	p.handlers[topic] = handler
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
	if p.transport == nil {
		return ErrNoTransport
	}
	if len(p.handlers) == 0 {
		return ErrNoHandlers
	}
	<-ctx.Done()
	return ctx.Err()
}
