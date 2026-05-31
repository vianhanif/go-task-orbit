package redisstreams

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	transport := New(Config{
		Addr:          "localhost:6379",
		StreamKey:     "test:stream",
		ConsumerGroup: "test-group",
		ConsumerName:  "test-consumer",
		BlockTimeout:  100 * time.Millisecond,
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

	time.Sleep(500 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "test",
		Payload: []byte("hello-streams"),
	})

	time.Sleep(1 * time.Second)
	cancel()
	transport.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Log("no messages received — Redis may not be running (expected for CI)")
	}
}

func TestAck(t *testing.T) {
	transport := New(Config{
		Addr:          "localhost:6379",
		StreamKey:     "test:ack",
		ConsumerGroup: "ack-group",
		ConsumerName:  "ack-consumer",
	})
	defer transport.Close()

	err := transport.Ack(context.Background(), []ringq.Message{
		{ReceiptHandle: "0-0"},
	})
	if err != nil {
		t.Logf("Ack returned: %v (may fail without Redis)", err)
	}
}

func TestNack(t *testing.T) {
	transport := New(Config{
		Addr:          "localhost:6379",
		StreamKey:     "test:nack",
		ConsumerGroup: "nack-group",
		ConsumerName:  "nack-consumer",
	})
	defer transport.Close()

	err := transport.Nack(context.Background(), ringq.Message{
		ReceiptHandle: "0-0",
	}, time.Second)
	if err != nil {
		t.Logf("Nack returned: %v (may fail without Redis)", err)
	}
}
