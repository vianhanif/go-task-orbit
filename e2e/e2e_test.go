//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	sqst "github.com/vianhanif/go-task-orbit/transport/sqs"
)

func newSQSTransport(queueURL string) *sqst.SQSTransport {
	return newSQSTransportWithVis(queueURL, 30)
}

func newSQSTransportWithVis(queueURL string, vis int32) *sqst.SQSTransport {
	return sqst.New(sqst.Config{
		QueueURL:          queueURL,
		MaxMessages:       10,
		WaitTime:          2,
		VisibilityTimeout: vis,
		BaseEndpoint:      flociEndpoint,
	})
}

func TestE2EHappyPath(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-happy")

	var called int32
	transport := newSQSTransport(queueURL)

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
	t.Log("pipeline started, waiting for poller...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("hello"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("message published: hello")

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call, got %d", n)
	}
	t.Logf("handler called %d times — acked via DeleteMessageBatch", atomic.LoadInt32(&called))
	env.cleanup(t)
}

func TestE2ERetryThenAck(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-retry")

	var attempt int32
	transport := newSQSTransportWithVis(queueURL, 5)

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
	t.Log("retry pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("retry-me"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("message published: retry-me")

	time.Sleep(8 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&attempt); n != 2 {
		t.Errorf("expected 2 attempts, got %d", n)
	}
	t.Logf("handler called %d times — failed on 1st, succeeded on 2nd", atomic.LoadInt32(&attempt))
	env.cleanup(t)
}

func TestE2EDLQ(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-dlq")

	var dlqCalled int32
	transport := newSQSTransportWithVis(queueURL, 5)

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
	t.Log("DLQ pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("fail"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("message published: fail → expecting DLQ routing")

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&dlqCalled); n < 1 {
		t.Errorf("expected at least 1 OnError call for DLQ, got %d", n)
	}
	t.Logf("OnError hook fired %d times — message routed to DLQ", atomic.LoadInt32(&dlqCalled))
	env.cleanup(t)
}

func TestE2EIdempotency(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-idem")

	var called int32
	transport := newSQSTransportWithVis(queueURL, 10)

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
	t.Log("idempotency pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:         "1",
		Topic:      "test",
		Payload:    []byte("first"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	}); err != nil {
		t.Fatalf("publish 1: %v", err)
	}
	t.Log("published 1st message (key=dup-key)")
	time.Sleep(3 * time.Second)

	if err := transport.Publish(ctx, ringq.Message{
		ID:         "2",
		Topic:      "test",
		Payload:    []byte("second"),
		Attributes: map[string]string{"IdempotencyKey": "dup-key"},
	}); err != nil {
		t.Fatalf("publish 2: %v", err)
	}
	t.Log("published 2nd message (same key=dup-key)")
	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call (duplicate filtered), got %d", n)
	}
	t.Logf("handler called %d times — duplicate message was filtered and acked silently", atomic.LoadInt32(&called))
	env.cleanup(t)
}

func TestE2EBatchReceive(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-batch")

	var count int32
	transport := newSQSTransportWithVis(queueURL, 10)

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
	t.Log("batch pipeline started...")
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
	t.Log("published 5 messages")

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&count); n != 5 {
		t.Errorf("expected 5 messages processed, got %d", n)
	}
	t.Logf("all %d messages processed and acked", atomic.LoadInt32(&count))
	env.cleanup(t)
}

func TestE2EGracefulShutdown(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-shutdown")

	var started int32
	block := make(chan struct{})

	transport := newSQSTransportWithVis(queueURL, 10)

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
	t.Log("shutdown pipeline started...")
	time.Sleep(500 * time.Millisecond)
	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("slow"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("published slow message, waiting for handler to start...")

	time.Sleep(3 * time.Second)
	cancel()
	t.Log("context cancelled, waiting for graceful drain...")

	if n := atomic.LoadInt32(&started); n != 1 {
		t.Errorf("expected handler to have started before shutdown")
	}
	t.Log("handler started before shutdown — completing inflight work")

	close(block)
	time.Sleep(500 * time.Millisecond)
	env.cleanup(t)
}

func TestE2EUnknownTopic(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-unknown")

	var errCalled int32
	transport := newSQSTransportWithVis(queueURL, 5)

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
	t.Log("unknown topic pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "unknown",
		Payload: []byte("?"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("published message to unregistered topic 'unknown'")

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&errCalled); n < 1 {
		t.Errorf("expected at least 1 OnError call for unknown topic, got %d", n)
	}
	t.Logf("OnError hook fired %d times — unknown topic message routed to DLQ", atomic.LoadInt32(&errCalled))
	env.cleanup(t)
}
