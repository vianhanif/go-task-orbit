package timerwheel

import (
	"context"
	"sync"
	"time"
)

const tickInterval = time.Second

type slot struct {
	target time.Time
	item   interface{}
}

type Wheel struct {
	mu      sync.Mutex
	buckets map[int64][]slot
	closed  bool
}

func New() *Wheel {
	return &Wheel{
		buckets: make(map[int64][]slot),
	}
}

func (w *Wheel) Insert(item interface{}, delay time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return
	}

	target := time.Now().Add(delay)
	tick := target.Unix()
	w.buckets[tick] = append(w.buckets[tick], slot{target: target, item: item})
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

	now := time.Now()
	result := make([]interface{}, 0)

	for t, slots := range w.buckets {
		if t > tick {
			continue
		}
		var keep []slot
		for _, s := range slots {
			if s.target.After(now) {
				keep = append(keep, s)
			} else {
				result = append(result, s.item)
			}
		}
		if len(keep) > 0 {
			w.buckets[t] = keep
		} else {
			delete(w.buckets, t)
		}
	}
	return result
}

func (w *Wheel) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()

	count := 0
	for _, slots := range w.buckets {
		count += len(slots)
	}
	return count
}

func (w *Wheel) Drain() []interface{} {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.closed = true

	result := make([]interface{}, 0)
	for _, slots := range w.buckets {
		for _, s := range slots {
			result = append(result, s.item)
		}
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
