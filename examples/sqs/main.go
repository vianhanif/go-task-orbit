package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/sqs"
)

// This example requires an SQS queue and DLQ to be pre-provisioned.
// For local testing, use Floci (task e2e starts it):
//
//	export AWS_ACCESS_KEY_ID=test
//	export AWS_SECRET_ACCESS_KEY=test
//	export AWS_SESSION_TOKEN=test
//
// Then create queues via AWS CLI pointing at localhost:4566.
//
// For production, set standard AWS credentials via IAM/IRSA.

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

	mainQueueURL := os.Getenv("SQS_QUEUE_URL")
	dlqURL := os.Getenv("SQS_DLQ_URL")
	if mainQueueURL == "" {
		fmt.Println("SQS_QUEUE_URL not set — using Floci default")
		mainQueueURL = "http://localhost:4566/000000000000/orders-main"
		dlqURL = "http://localhost:4566/000000000000/orders-dlq"
	}

	transport := sqs.New(sqs.Config{
		QueueURL:     mainQueueURL,
		DLQURL:       dlqURL,
		MaxMessages:  10,
		WaitTime:     20,
		BaseEndpoint: os.Getenv("SQS_ENDPOINT"),
	})

	pipeline := ringq.New().
		Transport(transport).
		Handle("order.created", ringq.Wrap(processOrder)).
		HandleWithRetry("payment.processed", ringq.Wrap(processOrder), 5, 2*time.Second, nil).
		Concurrency(32).
		BufferSize(4096)

	fmt.Println("Pipeline running... Press Ctrl+C to stop.")
	pipeline.Run(ctx)
}
