package ring

import (
	"errors"
	"sync"
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
	policy   OverflowPolicy
	closed   bool
}

func New(capacity int, policy OverflowPolicy) *Buffer {
	b := &Buffer{
		items:    make([]interface{}, capacity),
		capacity: capacity,
		policy:   policy,
	}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *Buffer) Enqueue(item interface{}) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return errors.New("ring: buffer is closed")
	}

	if b.count < b.capacity {
		b.items[b.head] = item
		b.head = (b.head + 1) % b.capacity
		b.count++
		b.cond.Signal()
		return nil
	}

	switch b.policy {
	case Block:
		for b.count == b.capacity && !b.closed {
			b.cond.Wait()
		}
		if b.closed {
			return errors.New("ring: buffer is closed")
		}
		b.items[b.head] = item
		b.head = (b.head + 1) % b.capacity
		b.count++
		b.cond.Signal()
		return nil

	case DropNewest:
		return nil

	case DropOldest:
		b.items[b.head] = item
		b.head = (b.head + 1) % b.capacity
		b.tail = (b.tail + 1) % b.capacity
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

	item := b.items[b.tail]
	b.items[b.tail] = nil
	b.tail = (b.tail + 1) % b.capacity
	b.count--
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
		result[i] = b.items[b.tail]
		b.items[b.tail] = nil
		b.tail = (b.tail + 1) % b.capacity
	}
	b.count -= batchSize
	b.cond.Signal()
	return result
}

func (b *Buffer) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.count
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
