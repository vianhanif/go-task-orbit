package ringq_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/idempotency"
	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

type dlqCaptureTransport struct {
	ringq.Transport
	mu  sync.Mutex
	dlq []ringq.Message
}

func (t *dlqCaptureTransport) SendToDLQ(ctx context.Context, msg ringq.Message) error {
	t.mu.Lock()
	t.dlq = append(t.dlq, msg)
	t.mu.Unlock()
	return t.Transport.SendToDLQ(ctx, msg)
}

func (t *dlqCaptureTransport) dlqLen() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.dlq)
}

func TestPipelineAck(t *testing.T) {
	var mu sync.Mutex
	var received []string

	tp := memory.New()
	p := ringq.New().
		Transport(tp).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			mu.Lock()
			received = append(received, string(raw))
			mu.Unlock()
			return ringq.Result{Action: ringq.Ack}
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)

	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("msg1")})
	tp.Publish(ctx, ringq.Message{ID: "2", Topic: "test", Payload: []byte("msg2")})

	time.Sleep(100 * time.Millisecond)
	cancel()

	mu.Lock()
	if len(received) != 2 {
		t.Errorf("expected 2 messages, got %d", len(received))
	}
	mu.Unlock()
}

func TestPipelineRetryThenAck(t *testing.T) {
	var attempt int32

	tp := memory.New()
		p := ringq.New().
		Transport(tp).
		HandleWithRetry("test", func(_ context.Context, raw []byte) ringq.Result {
			n := atomic.AddInt32(&attempt, 1)
			if n < 2 {
				return ringq.Result{Action: ringq.Retry}
			}
			return ringq.Result{Action: ringq.Ack}
		}, 3, time.Millisecond, nil).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("msg")})
	time.Sleep(300 * time.Millisecond)
	cancel()

	if n := atomic.LoadInt32(&attempt); n != 2 {
		t.Errorf("expected 2 attempts, got %d", n)
	}
}

func TestPipelineDLQ(t *testing.T) {
	tp := memory.New()
	dlqTp := &dlqCaptureTransport{Transport: tp}

	p := ringq.New().
		Transport(dlqTp).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			return ringq.Result{Action: ringq.DLQ}
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("fail")})
	time.Sleep(100 * time.Millisecond)
	cancel()

	if n := dlqTp.dlqLen(); n != 1 {
		t.Errorf("expected 1 DLQ message, got %d", n)
	}
}

func TestPipelineHooks(t *testing.T) {
	var onCompleteCalled int32
	var onErrorCalled int32

	tp := memory.New()
	p := ringq.New().
		Transport(tp).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			return ringq.Result{Action: ringq.Ack}
		}).
		WithHooks(ringq.Hooks{
			OnComplete: func(_ context.Context, _ string, _ time.Duration) {
				atomic.AddInt32(&onCompleteCalled, 1)
			},
			OnError: func(_ context.Context, _ string, _ error) {
				atomic.AddInt32(&onErrorCalled, 1)
			},
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("ok")})
	tp.Publish(ctx, ringq.Message{ID: "2", Topic: "nonexistent", Payload: []byte("unknown")})
	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := atomic.LoadInt32(&onCompleteCalled); n != 1 {
		t.Errorf("expected 1 OnComplete call, got %d", n)
	}
	if n := atomic.LoadInt32(&onErrorCalled); n != 1 {
		t.Errorf("expected 1 OnError call for unknown topic, got %d", n)
	}
}

func TestPipelineIdempotency(t *testing.T) {
	tp := memory.New()

	var callCount int32
	p := ringq.New().
		Transport(tp).
		Handle("test", func(_ context.Context, raw []byte) ringq.Result {
			atomic.AddInt32(&callCount, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Idempotency(ringq.IdempotencyConfig{
			Store:        idempotency.NewMemoryStore(),
			AttributeKey: "IdempotencyKey",
			TTL:          time.Hour,
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)

	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("dup"), Attributes: map[string]string{"IdempotencyKey": "same"}})
	time.Sleep(100 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "2", Topic: "test", Payload: []byte("dup2"), Attributes: map[string]string{"IdempotencyKey": "same"}})
	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := atomic.LoadInt32(&callCount); n != 1 {
		t.Errorf("expected 1 handler call (deduped), got %d", n)
	}
}

func TestPipelineNoTransport(t *testing.T) {
	p := ringq.New().Handle("test", func(_ context.Context, _ []byte) ringq.Result {
		return ringq.Result{Action: ringq.Ack}
	})

	err := p.Run(context.Background())
	if err != ringq.ErrNoTransport {
		t.Errorf("expected ErrNoTransport, got %v", err)
	}
}

func TestPipelineNoHandlers(t *testing.T) {
	tp := memory.New()
	p := ringq.New().Transport(tp)

	err := p.Run(context.Background())
	if err != ringq.ErrNoHandlers {
		t.Errorf("expected ErrNoHandlers, got %v", err)
	}
}

func TestPipelineGracefulShutdown(t *testing.T) {
	tp := memory.New()

	block := make(chan struct{})
	var started int32

	p := ringq.New().
		Transport(tp).
		Handle("test", func(_ context.Context, _ []byte) ringq.Result {
			atomic.StoreInt32(&started, 1)
			<-block
			return ringq.Result{Action: ringq.Ack}
		}).
		BufferSize(16).
		Concurrency(1)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "test", Payload: []byte("slow")})

	time.Sleep(50 * time.Millisecond)
	cancel()

	if n := atomic.LoadInt32(&started); n != 1 {
		t.Errorf("expected handler to have started")
	}

	close(block)
	time.Sleep(100 * time.Millisecond)
}

func TestPipelineUnknownTopic(t *testing.T) {
	tp := memory.New()
	dlqTp := &dlqCaptureTransport{Transport: tp}

	p := ringq.New().
		Transport(dlqTp).
		Handle("known", func(_ context.Context, _ []byte) ringq.Result {
			return ringq.Result{Action: ringq.Ack}
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "unknown", Payload: []byte("?")})
	time.Sleep(100 * time.Millisecond)
	cancel()

	if n := dlqTp.dlqLen(); n != 1 {
		t.Errorf("expected 1 DLQ call for unknown topic, got %d", n)
	}
}

func TestPipelineMultipleHandlers(t *testing.T) {
	tp := memory.New()

	var emails int32
	var invoices int32

	p := ringq.New().
		Transport(tp).
		Handle("email", func(_ context.Context, _ []byte) ringq.Result {
			atomic.AddInt32(&emails, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		Handle("invoice", func(_ context.Context, _ []byte) ringq.Result {
			atomic.AddInt32(&invoices, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		BufferSize(16).
		Concurrency(2)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(20 * time.Millisecond)
	tp.Publish(ctx, ringq.Message{ID: "1", Topic: "email", Payload: []byte("e1")})
	tp.Publish(ctx, ringq.Message{ID: "2", Topic: "invoice", Payload: []byte("i1")})
	tp.Publish(ctx, ringq.Message{ID: "3", Topic: "email", Payload: []byte("e2")})
	time.Sleep(200 * time.Millisecond)
	cancel()

	if n := atomic.LoadInt32(&emails); n != 2 {
		t.Errorf("expected 2 email handler calls, got %d", n)
	}
	if n := atomic.LoadInt32(&invoices); n != 1 {
		t.Errorf("expected 1 invoice handler call, got %d", n)
	}
}
