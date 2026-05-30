//go:build e2e_gcp

package e2e

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestE2EGCPHappyPath(t *testing.T) {
	env := setupPubSubEnv(t)

	var called int32
	transport := env.createTransport(t)

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&called, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("hello-gcp"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGCPRetry(t *testing.T) {
	env := setupPubSubEnv(t)

	var attempt int32
	transport := env.createTransport(t)

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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("retry-me"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(8 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&attempt); n != 2 {
		t.Errorf("expected 2 attempts, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGCPDLQ(t *testing.T) {
	env := setupPubSubEnv(t)

	var dlqCalled int32
	transport := env.createTransport(t)

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			return ringq.Result{Action: ringq.DLQ, Err: fmt.Errorf("unrecoverable")}
		}).
		WithHooks(ringq.Hooks{
			OnError: func(_ context.Context, topic string, err error) {
				atomic.AddInt32(&dlqCalled, 1)
			},
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("fail"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&dlqCalled); n < 1 {
		t.Errorf("expected at least 1 OnError call for DLQ, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGCPIdempotency(t *testing.T) {
	env := setupPubSubEnv(t)

	var called int32
	transport := env.createTransport(t)

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

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:         "1",
		Topic:      "test",
		Payload:    []byte("first"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	time.Sleep(3 * time.Second)

	if err := transport.Publish(ctx, ringq.Message{
		ID:         "2",
		Topic:      "test",
		Payload:    []byte("second"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call (duplicate filtered), got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGCPBatch(t *testing.T) {
	env := setupPubSubEnv(t)

	var count int32
	transport := env.createTransport(t)

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&count, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(2).
		BufferSize(32)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	for i := 0; i < 5; i++ {
		if err := transport.Publish(ctx, ringq.Message{
			ID:      fmt.Sprintf("%d", i),
			Topic:   "test",
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		}); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&count); n != 5 {
		t.Errorf("expected 5 messages processed, got %d", n)
	}
	env.cleanup(t)
}

func TestE2EGCPUnknownTopic(t *testing.T) {
	env := setupPubSubEnv(t)

	var errCalled int32
	transport := env.createTransport(t)

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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "unknown",
		Payload: []byte("?"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&errCalled); n < 1 {
		t.Errorf("expected at least 1 OnError call for unknown topic, got %d", n)
	}
	env.cleanup(t)
}
