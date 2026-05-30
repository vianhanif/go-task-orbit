package executor

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolExecutesTasks(t *testing.T) {
	p := New(4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	var counter int32
	for i := 0; i < 10; i++ {
		p.Submit(func() {
			atomic.AddInt32(&counter, 1)
		})
	}

	p.Stop()
	p.Wait()

	if n := atomic.LoadInt32(&counter); n != 10 {
		t.Errorf("expected 10 tasks executed, got %d", n)
	}
}

func TestPoolConcurrencyLimit(t *testing.T) {
	p := New(2)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	var concurrent int32
	var maxConcurrent int32

	for i := 0; i < 10; i++ {
		p.Submit(func() {
			c := atomic.AddInt32(&concurrent, 1)
			for {
				prev := atomic.LoadInt32(&maxConcurrent)
				if c > prev {
					if atomic.CompareAndSwapInt32(&maxConcurrent, prev, c) {
						break
					}
				} else {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&concurrent, -1)
		})
	}

	p.Stop()
	p.Wait()

	if maxConcurrent > 2 {
		t.Errorf("expected max concurrency <= 2, got %d", maxConcurrent)
	}
}

func TestPoolTrySubmit(t *testing.T) {
	p := New(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	p.Start(ctx)

	started := make(chan struct{})
	block := make(chan struct{})
	p.Submit(func() {
		close(started)
		<-block
	})

	<-started

	if !p.TrySubmit(func() {}) {
		t.Error("expected TrySubmit to succeed with buffered slot")
	}

	block2 := make(chan struct{})
	p.TrySubmit(func() {
		<-block2
	})

	if p.TrySubmit(func() {}) {
		t.Error("expected TrySubmit to fail when all workers busy")
	}

	close(block)
	close(block2)
	p.Stop()
	p.Wait()
}

func TestPoolStopWithNoStart(t *testing.T) {
	p := New(2)
	p.Stop()
	p.Wait()
}
