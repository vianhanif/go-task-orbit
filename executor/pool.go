package executor

import (
	"sync"
)

type Pool struct {
	workCh  chan func()
	wg      sync.WaitGroup
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

func (p *Pool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}
	p.started = true

	for i := 0; i < cap(p.workCh); i++ {
		p.wg.Add(1)
		go p.worker()
	}
}

func (p *Pool) worker() {
	defer p.wg.Done()
	for fn := range p.workCh {
		fn()
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

// StopDispatching stops the pool from accepting new tasks
// by closing the work channel. All tasks already submitted
// will be processed before workers exit. Do not call Submit
// or TrySubmit after StopDispatching.
func (p *Pool) StopDispatching() {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()

	if !started {
		return
	}
	close(p.workCh)
}

func (p *Pool) Wait() {
	p.wg.Wait()
}
