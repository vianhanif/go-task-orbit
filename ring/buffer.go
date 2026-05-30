package ring

import (
	"errors"
	"sync"
	"sync/atomic"
)

var ErrBufferFull = errors.New("ring: buffer is full")

type Buffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    []interface{}
	head     int
	tail     int
	count    int
	capacity int
	mask     int
	policy   OverflowPolicy
	closed   bool
	_head    int64
	_tail    int64
}

func New(capacity int, policy OverflowPolicy) *Buffer {
	c := nextPowerOfTwo(capacity)
	b := &Buffer{
		items:    make([]interface{}, c),
		capacity: c,
		mask:     c - 1,
		policy:   policy,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func nextPowerOfTwo(v int) int {
	if v <= 1 {
		return 1
	}
	v--
	v |= v >> 1
	v |= v >> 2
	v |= v >> 4
	v |= v >> 8
	v |= v >> 16
	v++
	return v
}

func (b *Buffer) Enqueue(item interface{}) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrBufferFull
	}

	if b.count < b.capacity {
		b.items[b.head&b.mask] = item
		b.head++
		b.count++
		atomic.StoreInt64(&b._head, int64(b.head))
		b.cond.Signal()
		return nil
	}

	switch b.policy {
	case Block:
		for b.count == b.capacity && !b.closed {
			b.cond.Wait()
		}
		if b.closed {
			return ErrBufferFull
		}
		b.items[b.head&b.mask] = item
		b.head++
		b.count++
		atomic.StoreInt64(&b._head, int64(b.head))
		b.cond.Signal()
		return nil

	case DropNewest:
		return nil

	case DropOldest:
		b.items[b.head&b.mask] = item
		b.head++
		b.tail++
		atomic.StoreInt64(&b._head, int64(b.head))
		atomic.StoreInt64(&b._tail, int64(b.tail))
		b.cond.Signal()
		return nil

	case Reject:
		return ErrBufferFull

	default:
		return ErrBufferFull
	}
}

func (b *Buffer) EnqueueBatch(items []interface{}) (int, error) {
	for i, item := range items {
		if err := b.Enqueue(item); err != nil {
			return i, err
		}
	}
	return len(items), nil
}

func (b *Buffer) Dequeue() (interface{}, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.count == 0 {
		return nil, false
	}

	item := b.items[b.tail&b.mask]
	b.items[b.tail&b.mask] = nil
	b.tail++
	b.count--
	atomic.StoreInt64(&b._tail, int64(b.tail))
	b.cond.Signal()
	return item, true
}

func (b *Buffer) DequeueBatch(max int) []interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.count == 0 || max == 0 {
		return nil
	}

	batchSize := b.count
	if max > 0 && max < batchSize {
		batchSize = max
	}

	result := make([]interface{}, batchSize)
	for i := 0; i < batchSize; i++ {
		result[i] = b.items[b.tail&b.mask]
		b.items[b.tail&b.mask] = nil
		b.tail++
	}
	b.count -= batchSize
	atomic.StoreInt64(&b._tail, int64(b.tail))
	b.cond.Broadcast()
	return result
}

func (b *Buffer) Len() int {
	head := atomic.LoadInt64(&b._head)
	tail := atomic.LoadInt64(&b._tail)
	return int(head - tail)
}

func (b *Buffer) Cap() int {
	return b.capacity
}

func (b *Buffer) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	b.cond.Broadcast()
}
