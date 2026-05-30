package memory

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestPublishAndSubscribe(t *testing.T) {
	tp := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received int32
	done := make(chan struct{})

	go func() {
		tp.Subscribe(ctx, func(_ context.Context, msgs []ringq.Message) error {
			atomic.AddInt32(&received, int32(len(msgs)))
			close(done)
			return nil
		})
	}()

	tp.Publish(ctx, ringq.Message{ID: "1", Payload: []byte("hello")})

	<-done

	if n := atomic.LoadInt32(&received); n != 1 {
		t.Errorf("expected 1 message, got %d", n)
	}
}

func TestMultipleMessages(t *testing.T) {
	tp := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var received int32
	done := make(chan struct{})

	go func() {
		tp.Subscribe(ctx, func(_ context.Context, msgs []ringq.Message) error {
			atomic.AddInt32(&received, int32(len(msgs)))
			if atomic.LoadInt32(&received) >= 3 {
				close(done)
			}
			return nil
		})
	}()

	tp.Publish(ctx, ringq.Message{ID: "1"})
	tp.Publish(ctx, ringq.Message{ID: "2"})
	tp.Publish(ctx, ringq.Message{ID: "3"})

	<-done

	if n := atomic.LoadInt32(&received); n != 3 {
		t.Errorf("expected 3 messages, got %d", n)
	}
}

func TestClose(t *testing.T) {
	tp := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tp.Subscribe(ctx, nil)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	tp.Close()
}

func TestCloseUnblocksSubscribe(t *testing.T) {
	tp := New()
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		tp.Subscribe(ctx, nil)
		close(done)
	}()

	tp.Close()
	<-done
}

func TestAckNoop(t *testing.T) {
	tp := New()
	if err := tp.Ack(context.Background(), nil); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	tp.Close()
}

func TestSendToDLQNoop(t *testing.T) {
	tp := New()
	if err := tp.SendToDLQ(context.Background(), ringq.Message{}); err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	tp.Close()
}
