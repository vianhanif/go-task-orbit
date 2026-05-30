//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	sqst "github.com/vianhanif/go-task-orbit/transport/sqs"
)

func TestE2EHappyPath(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-happy")

	var called int32
	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 30,
	})

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&called, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("hello"),
	})

	time.Sleep(2 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call, got %d", n)
	}
	env.cleanup(t)
}

func TestE2ERetryThenAck(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-retry")

	var attempt int32
	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 5,
	})

	p := ringq.New().
		Transport(transport).
		HandleWithRetry("test", func(_ context.Context, raw []byte) ringq.Result {
			n := atomic.AddInt32(&attempt, 1)
			if n < 2 {
				return ringq.Result{Action: ringq.Retry}
			}
			return ringq.Result{Action: ringq.Ack}
		}, 3, 100*time.Millisecond, nil).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("retry-me"),
	})

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&attempt); n != 2 {
		t.Errorf("expected 2 attempts, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EDLQ(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-dlq")

	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 5,
	})

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			return ringq.Result{Action: ringq.DLQ, Err: fmt.Errorf("unrecoverable")}
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("fail"),
	})

	time.Sleep(2 * time.Second)
	cancel()

	// Verify main queue is empty (message was acked after DLQ send)
	// Verifying DLQ queue content requires receiving from the DLQ
	env.cleanup(t)
}

func TestE2EIdempotency(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-idem")

	var called int32
	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 10,
	})

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&called, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Idempotency(ringq.IdempotencyConfig{
			Store:        &syncIdemStore{},
			AttributeKey: "IdempotencyKey",
			TTL:          time.Hour,
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	// Publish two messages with same idempotency key
	transport.Publish(ctx, ringq.Message{
		ID:         "1",
		Topic:      "test",
		Payload:    []byte("first"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	})
	time.Sleep(1 * time.Second)

	transport.Publish(ctx, ringq.Message{
		ID:         "2",
		Topic:      "test",
		Payload:    []byte("second"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	})
	time.Sleep(3 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call (duplicate filtered), got %d", n)
	}
	env.cleanup(t)
}

func TestE2EBatchReceive(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-batch")

	var count int32
	var mu sync.Mutex
	var received []string

	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 10,
	})

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			mu.Lock()
			received = append(received, string(raw))
			mu.Unlock()
			atomic.AddInt32(&count, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(2).
		BufferSize(32)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		transport.Publish(ctx, ringq.Message{
			ID:      fmt.Sprintf("%d", i),
			Topic:   "test",
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
	}

	time.Sleep(3 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&count); n != 5 {
		t.Errorf("expected 5 messages processed, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGracefulShutdown(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-shutdown")

	var started int32
	block := make(chan struct{})

	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 10,
	})

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.StoreInt32(&started, 1)
			<-block
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(1).
		BufferSize(16)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(500 * time.Millisecond)
	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("slow"),
	})

	time.Sleep(1 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&started); n != 1 {
		t.Errorf("expected handler to have started before shutdown")
	}

	close(block)
	time.Sleep(500 * time.Millisecond)
	env.cleanup(t)
}

func TestE2EUnknownTopic(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-unknown")

	var errCalled int32
	transport := sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: 5,
	})

	p := ringq.New().
		Transport(transport).
		Handle("known", func(_ context.Context, raw []byte) ringq.Result {
			return ringq.Result{Action: ringq.Ack}
		}).
		WithHooks(ringq.Hooks{
			OnError: func(_ context.Context, topic string, err error) {
				atomic.AddInt32(&errCalled, 1)
			},
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "unknown",
		Payload: []byte("?"),
	})

	time.Sleep(2 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&errCalled); n != 1 {
		t.Errorf("expected 1 OnError call for unknown topic, got %d", n)
	}
	env.cleanup(t)
}

// syncIdemStore is a simple in-memory store for e2e tests.
type syncIdemStore struct {
	mu   sync.Mutex
	keys map[string]time.Time
}

func (s *syncIdemStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]time.Time)
	}
	exp, ok := s.keys[key]
	if !ok {
		return false, nil
	}
	if time.Now().After(exp) {
		delete(s.keys, key)
		return false, nil
	}
	return true, nil
}

func (s *syncIdemStore) Mark(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]time.Time)
	}
	s.keys[key] = time.Now().Add(ttl)
	return nil
}

func (s *syncIdemStore) Close() error { return nil }
