package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

type OrderPayload struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func processOrder(ctx context.Context, msg OrderPayload) ringq.Result {
	fmt.Printf("Processing order %s for $%.2f\n", msg.OrderID, msg.Amount)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	memTransport := memory.New()

	pipeline := ringq.New().
		Transport(memTransport).
		Handle("order.created", ringq.Wrap(processOrder)).
		Concurrency(4).
		BufferSize(1024)

	go pipeline.Run(ctx)

	memTransport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "order.created",
		Payload: []byte(`{"order_id":"ORD-001","amount":99.95}`),
	})
	memTransport.Publish(ctx, ringq.Message{
		ID:      "2",
		Topic:   "order.created",
		Payload: []byte(`{"order_id":"ORD-002","amount":149.50}`),
	})

	time.Sleep(100 * time.Millisecond)
	fmt.Println("Done. Press Ctrl+C to exit.")
	<-ctx.Done()
}
