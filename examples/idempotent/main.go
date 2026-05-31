package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/idempotency"
	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

type Order struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func processOrder(ctx context.Context, msg Order) ringq.Result {
	fmt.Printf("Processing order %s for $%.2f\n", msg.OrderID, msg.Amount)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	transport := memory.New()

	pipeline := ringq.New().
		Transport(transport).
		Handle("order.created", ringq.Wrap(processOrder)).
		Idempotency(ringq.IdempotencyConfig{
			Store:        idempotency.NewMemoryStore(),
			AttributeKey: "IdempotencyKey",
			TTL:          1 * time.Hour,
		}).
		WithHooks(ringq.Hooks{
			OnDuplicate: func(_ context.Context, key string) {
				fmt.Printf("Duplicate message filtered: key=%s\n", key)
			},
		}).
		Concurrency(2).
		BufferSize(16)

	go pipeline.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// First message — will be processed
	transport.Publish(ctx, ringq.Message{
		ID:         "1",
		Topic:      "order.created",
		Payload:    []byte(`{"order_id":"ORD-001","amount":99.95}`),
		Attributes: map[string]string{"IdempotencyKey": "ord-001-v1"},
	})

	time.Sleep(300 * time.Millisecond)

	// Duplicate — will be filtered and acked silently
	transport.Publish(ctx, ringq.Message{
		ID:         "2",
		Topic:      "order.created",
		Payload:    []byte(`{"order_id":"ORD-001","amount":99.95}`),
		Attributes: map[string]string{"IdempotencyKey": "ord-001-v1"},
	})

	time.Sleep(300 * time.Millisecond)

	// New unique order — will be processed
	transport.Publish(ctx, ringq.Message{
		ID:         "3",
		Topic:      "order.created",
		Payload:    []byte(`{"order_id":"ORD-002","amount":149.50}`),
		Attributes: map[string]string{"IdempotencyKey": "ord-002-v1"},
	})

	time.Sleep(300 * time.Millisecond)
	fmt.Println("Done. Press Ctrl+C to exit.")
	<-ctx.Done()
}
