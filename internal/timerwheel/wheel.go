package timerwheel

import (
	"context"
	"sync"
	"time"
)

const tickInterval = time.Second

type Wheel struct {
	mu      sync.Mutex
	buckets map[int64][]interface{}
	current int64
	closed  bool
}

func New() *Wheel {
	return &Wheel{
		buckets: make(map[int64][]interface{}),
	}
}

func (w *Wheel) Insert(item interface{}, delay time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	tick := time.Now().Add(delay).Unix()
	w.buckets[tick] = append(w.buckets[tick], item)
}

func (w *Wheel) Start(ctx context.Context) <-chan []interface{} {
	out := make(chan []interface{}, 64)

	go func() {
		defer close(out)

		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				batch := w.expire(now.Unix())
				if len(batch) > 0 {
					select {
					case out <- batch:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return out
}

func (w *Wheel) expire(tick int64) []interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	result := make([]interface{}, 0)
	for t, items := range w.buckets {
		if t <= tick && len(items) > 0 {
			result = append(result, items...)
			delete(w.buckets, t)
		}
	}
	return result
}

func (w *Wheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	count := 0
	for _, items := range w.buckets {
		count += len(items)
	}
	return count
}

func (w *Wheel) Drain() []interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true

	result := make([]interface{}, 0)
	for _, items := range w.buckets {
		result = append(result, items...)
	}
	w.buckets = nil
	return result
}

func (w *Wheel) Close() {
	w.mu.Lock()
	w.closed = true
	w.buckets = nil
	w.mu.Unlock()
}
