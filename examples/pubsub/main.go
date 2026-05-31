package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/pubsub"
)

// This example requires a GCP Pub/Sub topic and subscription to be pre-provisioned.
// For local testing, use the Google Pub/Sub emulator:
//
//	gcloud beta emulators pubsub start --project=test-project
//	export PUBSUB_EMULATOR_HOST=localhost:8085
//
// For production, set GOOGLE_APPLICATION_CREDENTIALS or use Workload Identity on GKE.

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

	projectID := os.Getenv("GCP_PROJECT_ID")
	if projectID == "" {
		projectID = "test-project"
	}

	topicID := os.Getenv("PUBSUB_TOPIC_ID")
	subscriptionID := os.Getenv("PUBSUB_SUBSCRIPTION_ID")
	if topicID == "" {
		topicID = "orders-topic"
		subscriptionID = "orders-sub"
	}

	transport, err := pubsub.New(ctx, pubsub.Config{
		ProjectID:      projectID,
		TopicID:        topicID,
		SubscriptionID: subscriptionID,
		MaxMessages:    10,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create pubsub transport: %v\n", err)
		os.Exit(1)
	}
	defer transport.Close()

	pipeline := ringq.New().
		Transport(transport).
		Handle("order.created", ringq.Wrap(processOrder)).
		Concurrency(32).
		BufferSize(4096)

	fmt.Println("Pipeline running... Press Ctrl+C to stop.")
	pipeline.Run(ctx)
}
