//go:build e2e_gcp

package e2e

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestE2EGCPETADelayedTask(t *testing.T) {
	env := setupPubSubEnv(t)

	var called int32
	var callTime time.Time
	transport := env.createTransport(t)

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
	t.Log("GCP pipeline started with timer wheel...")
	time.Sleep(500 * time.Millisecond)

	publishTime := time.Now()
	if err := transport.Publish(ctx, ringq.Message{
		ID:        "1",
		Topic:     "test",
		Payload:   []byte("delayed-gcp"),
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
		if elapsed < 3*time.Second {
			t.Errorf("handler called too soon: %v after publish (expected >= 3s)", elapsed)
		}
	}
	env.cleanup(t)
}

func TestE2EGCPETABackoff(t *testing.T) {
	env := setupPubSubEnv(t)

	var attempt int32
	var callTimes []time.Time
	transport := env.createTransport(t)

	p := ringq.New().
		Transport(transport).
		HandleWithRetry("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&attempt, 1)
			callTimes = append(callTimes, time.Now())
			return ringq.Result{Action: ringq.Retry}
		}, 3, 1*time.Second, nil).
		Concurrency(2).
		BufferSize(16)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	go p.Run(ctx)
	t.Log("GCP backoff pipeline started...")
	time.Sleep(500 * time.Millisecond)

	if err := transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("backoff-gcp"),
	}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	t.Log("published — expecting exponential backoff: 1s, 2s, 4s, DLQ")

	time.Sleep(20 * time.Second)
	cancel()

	n := atomic.LoadInt32(&attempt)
	t.Logf("handler called %d times (expected >= 3 before DLQ)", n)
	if n < 3 {
		t.Errorf("expected at least 3 attempts before DLQ, got %d", n)
	}

	if len(callTimes) >= 3 {
		d1 := callTimes[1].Sub(callTimes[0])
		d2 := callTimes[2].Sub(callTimes[1])
		t.Logf("delay 1→2: %v (expected ~1s)", d1)
		t.Logf("delay 2→3: %v (expected ~2s)", d2)
		if d2 < d1 {
			t.Errorf("expected growing delays, got d1=%v d2=%v", d1, d2)
		}
	}

	env.cleanup(t)
}
