package executor

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolExecutesTasks(t *testing.T) {
	p := New(4)
	p.Start()

	var counter int32
	for i := 0; i < 10; i++ {
		p.Submit(func() {
			atomic.AddInt32(&counter, 1)
		})
	}

	p.StopDispatching()
	p.Wait()

	if n := atomic.LoadInt32(&counter); n != 10 {
		t.Errorf("expected 10 tasks executed, got %d", n)
	}
}

func TestPoolConcurrencyLimit(t *testing.T) {
	p := New(2)
	p.Start()

	var concurrent int32
	var maxConcurrent int32

	for i := 0; i < 5; i++ {
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

	p.StopDispatching()
	p.Wait()

	if maxConcurrent > 2 {
		t.Errorf("expected max concurrency <= 2, got %d", maxConcurrent)
	}
}

func TestPoolTrySubmit(t *testing.T) {
	p := New(1)
	p.Start()

	started := make(chan struct{})
	block := make(chan struct{})

	p.Submit(func() {
		close(started)
		<-block
	})

	<-started

	if !p.TrySubmit(func() {}) {
		t.Error("expected TrySubmit to succeed")
	}

	block2 := make(chan struct{})
	p.TrySubmit(func() {
		<-block2
	})

	if p.TrySubmit(func() {}) {
		t.Error("expected TrySubmit to fail when queue is full")
	}

	close(block)
	close(block2)

	p.StopDispatching()
	p.Wait()
}

func TestPoolStopWithNoStart(t *testing.T) {
	p := New(2)
	p.StopDispatching()
	p.Wait()
}
