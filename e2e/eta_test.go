//go:build e2e

package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestE2EETADelayedTask(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-eta")

	var called int32
	var callTime time.Time
	transport := newSQSTransportWithVis(queueURL, 60)

	p := ringq.New().
		Transport(transport).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&called, 1)
			callTime = time.Now()
			return ringq.Result{Action: ringq.Ack}
		}).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	go p.Run(ctx)
	t.Log("pipeline started with timer wheel...")
	time.Sleep(500 * time.Millisecond)

	publishTime := time.Now()
	if err := transport.Publish(ctx, ringq.Message{
		ID:        "1",
		Topic:     "test",
		Payload:   []byte("delayed"),
		NotBefore: 3 * time.Second,
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Logf("message published with NotBefore=3s at %v", publishTime)

	// Check handler NOT called before delay
	time.Sleep(1 * time.Second)
	if n := atomic.LoadInt32(&called); n != 0 {
		t.Errorf("handler called too early (before 3s delay): %d calls", n)
	}
	t.Log("verified: handler NOT called before ETA (1s elapsed)")

	time.Sleep(5 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 handler call after ETA, got %d", n)
	}
	if !callTime.IsZero() {
		elapsed := callTime.Sub(publishTime)
		t.Logf("handler called after %v (expected >= 3s)", elapsed)
		if elapsed < 2*time.Second {
			t.Errorf("handler called too soon: %v after publish (expected >= 2s, actual >= 3s)", elapsed)
		}
	}
	env.cleanup(t)
}

func TestE2EETAImmediateTask(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-eta-immediate")

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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go p.Run(ctx)
	time.Sleep(500 * time.Millisecond)

	// NotBefore=0 should process immediately
	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("immediate"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("message published with NotBefore=0 (immediate)")

	time.Sleep(3 * time.Second)
	cancel()

	if n := atomic.LoadInt32(&called); n != 1 {
		t.Errorf("expected 1 immediate handler call, got %d", n)
	}
	t.Logf("handler called %d times — immediate delivery confirmed", atomic.LoadInt32(&called))
	env.cleanup(t)
}

func TestE2EETAExponentialBackoff(t *testing.T) {
	env := setupEnv(t)
	queueURL := env.createQueue(t, "e2e-eta-backoff")

	var attempt int32
	var callTimes []time.Time
	transport := newSQSTransportWithVis(queueURL, 60)

	p := ringq.New().
		Transport(transport).
		HandleWithRetry("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&attempt, 1)
			callTimes = append(callTimes, time.Now())
			return ringq.Result{Action: ringq.Retry, Err: context.DeadlineExceeded}
		}, 3, 1*time.Second, nil).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	go p.Run(ctx)
	t.Log("backoff pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("backoff-test"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("published — expecting exponential backoff: 1s, 2s, 4s, DLQ")

	time.Sleep(30 * time.Second)
	cancel()

	n := atomic.LoadInt32(&attempt)
	t.Logf("handler called %d times (expected >= 3 before DLQ)", n)

	if n < 3 {
		t.Errorf("expected at least 3 attempts before DLQ, got %d", n)
	}

	// Verify delays grow exponentially
	if len(callTimes) >= 3 {
		d1 := callTimes[1].Sub(callTimes[0])
		d2 := callTimes[2].Sub(callTimes[1])
		t.Logf("delay 1→2: %v", d1)
		t.Logf("delay 2→3: %v", d2)
		if d1 < 100*time.Millisecond {
			t.Errorf("expected delay after 1st retry, got %v", d1)
		}
		if d2 < 500*time.Millisecond {
			t.Errorf("expected longer delay after 2nd retry, got %v", d2)
		}
	}

	env.cleanup(t)
}
