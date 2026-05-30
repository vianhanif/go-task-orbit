//go:build e2e_gcp

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"

	ps "github.com/vianhanif/go-task-orbit/transport/pubsub"
)

type gcpTestEnv struct {
	projectID      string
	topicID        string
	subscriptionID string
	transport      *ps.PubSubTransport
	client         *pubsub.Client
}

func setupPubSubEnv(t *testing.T) *gcpTestEnv {
	t.Helper()

	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:4588")
	}

	projectID := "test-project"
	topicID := fmt.Sprintf("e2e-topic-%d", time.Now().UnixNano())
	subscriptionID := fmt.Sprintf("e2e-sub-%d", time.Now().UnixNano())

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		t.Skipf("pubsub emulator not reachable: %v", err)
	}

	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}

	_, err = client.CreateSubscription(ctx, subscriptionID, pubsub.SubscriptionConfig{
		Topic: topic,
	})
	if err != nil {
		t.Fatalf("create subscription: %v", err)
	}

	return &gcpTestEnv{
		projectID:      projectID,
		topicID:        topicID,
		subscriptionID: subscriptionID,
		client:         client,
	}
}

func (e *gcpTestEnv) createTransport(t *testing.T) *ps.PubSubTransport {
	t.Helper()

	tr := ps.NewWithClient(e.client, ps.Config{
		ProjectID:      e.projectID,
		TopicID:        e.topicID,
		SubscriptionID: e.subscriptionID,
		MaxMessages:    10,
	})

	e.transport = tr
	return tr
}

func (e *gcpTestEnv) cleanup(t *testing.T) {
	t.Helper()
	if e.transport != nil {
		e.transport.Close()
	}
	if e.client != nil {
		e.client.Close()
	}
}
