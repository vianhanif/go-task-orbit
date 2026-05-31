package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

// This example demonstrates exponential backoff retry with a DLQ fallback.
// The handler is configured to fail 3 times, then succeed on the 4th attempt.
// Delays grow exponentially: 1s → 2s → 4s → success.
// After 10 failed attempts (max retries), the message is routed to DLQ.

type PaymentPayload struct {
	PaymentID string  `json:"payment_id"`
	Amount    float64 `json:"amount"`
}

var attemptCount int

func processPayment(ctx context.Context, msg PaymentPayload) ringq.Result {
	attemptCount++
	if attemptCount < 4 {
		err := errors.New("temporary payment gateway error")
		fmt.Printf("Attempt %d: payment %s failed — will retry with backoff (%v)\n", attemptCount, msg.PaymentID, err)
		return ringq.Result{Action: ringq.Retry, Err: err}
	}
	fmt.Printf("Attempt %d: payment %s processed successfully\n", attemptCount, msg.PaymentID)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	transport := memory.New()

	pipeline := ringq.New().
		Transport(transport).
		HandleWithRetry("payment.process", ringq.Wrap(processPayment), 10, 1*time.Second, nil).
		WithHooks(ringq.Hooks{
			OnRetry: func(_ context.Context, topic string, msg ringq.Message, attempt int) {
				fmt.Printf("[RETRY] topic=%s attempt=%d (delay grows: 1s → 2s → 4s... cap 5min)\n", topic, attempt)
			},
			OnError: func(_ context.Context, topic string, err error) {
				fmt.Printf("[ERROR] topic=%s err=%v\n", topic, err)
			},
			OnComplete: func(_ context.Context, topic string, dur time.Duration) {
				fmt.Printf("[DONE] topic=%s completed in %v\n", topic, dur)
			},
		}).
		Concurrency(2).
		BufferSize(16)

	go pipeline.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "payment.process",
		Payload: []byte(`{"payment_id":"PAY-001","amount":99.95}`),
	})

	time.Sleep(15 * time.Second)
	cancel()
	fmt.Println("Done.")
}
