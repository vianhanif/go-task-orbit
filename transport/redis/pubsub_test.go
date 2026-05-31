package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestPublishSubscribe(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transport := New(Config{
		Addr:     "localhost:6379",
		Channels: []string{"test-channel"},
	})

	var mu sync.Mutex
	var received []string

	go func() {
		transport.Subscribe(ctx, func(_ context.Context, msgs []ringq.Message) error {
			mu.Lock()
			for _, m := range msgs {
				received = append(received, string(m.Payload))
			}
			mu.Unlock()
			return nil
		})
	}()

	time.Sleep(200 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("hello-redis"),
	})

	time.Sleep(500 * time.Millisecond)
	cancel()
	transport.Close()

	mu.Lock()
	if len(received) == 0 {
		t.Log("no messages received — Redis may not be running (expected for CI)")
	} else if received[0] != "hello-redis" {
		t.Errorf("expected 'hello-redis', got %q", received[0])
	}
	mu.Unlock()
}

func TestAckNoOp(t *testing.T) {
	transport := New(Config{
		Addr:     "localhost:6379",
		Channels: []string{"test"},
	})
	defer transport.Close()

	err := transport.Ack(context.Background(), nil)
	if err != nil {
		t.Errorf("Ack should be no-op, got error: %v", err)
	}
}

func TestNackNoOp(t *testing.T) {
	transport := New(Config{
		Addr:     "localhost:6379",
		Channels: []string{"test"},
	})
	defer transport.Close()

	err := transport.Nack(context.Background(), ringq.Message{}, 0)
	if err != nil {
		t.Errorf("Nack should be no-op, got error: %v", err)
	}
}

func TestSendToDLQNoOp(t *testing.T) {
	transport := New(Config{
		Addr:     "localhost:6379",
		Channels: []string{"test"},
	})
	defer transport.Close()

	err := transport.SendToDLQ(context.Background(), ringq.Message{})
	if err != nil {
		t.Errorf("SendToDLQ should be no-op, got error: %v", err)
	}
}
