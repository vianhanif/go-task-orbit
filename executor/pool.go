package executor

import (
	"context"
	"sync"
)

type Pool struct {
	workCh  chan func()
	wg      sync.WaitGroup
	cancel  context.CancelFunc
	started bool
	mu      sync.Mutex
}

func New(size int) *Pool {
	if size < 1 {
		size = 1
	}
	return &Pool{
		workCh: make(chan func(), size),
	}
}

func (p *Pool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}
	p.started = true

	ctx, cancel := context.WithCancel(ctx)
	p.cancel = cancel

	capacity := cap(p.workCh)
	p.wg.Add(capacity)
	for i := 0; i < capacity; i++ {
		go p.worker(ctx)
	}
}

func (p *Pool) worker(ctx context.Context) {
	defer p.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case fn, ok := <-p.workCh:
			if !ok {
				return
			}
			fn()
		}
	}
}

func (p *Pool) Submit(fn func()) {
	p.workCh <- fn
}

func (p *Pool) TrySubmit(fn func()) bool {
	select {
	case p.workCh <- fn:
		return true
	default:
		return false
	}
}

func (p *Pool) Stop() {
	p.mu.Lock()
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Unlock()
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
