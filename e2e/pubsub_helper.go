//go:build e2e_gcp

package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/transport/pubsub"
)

type gcpTestEnv struct {
	projectID      string
	topicID        string
	subscriptionID string
	transport      *pubsub.PubSubTransport
}

func setupPubSubEnv(t *testing.T) *gcpTestEnv {
	t.Helper()

	if os.Getenv("PUBSUB_EMULATOR_HOST") == "" {
		os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:4588")
	}

	projectID := "test-project"
	topicID := fmt.Sprintf("e2e-topic-%d", time.Now().UnixNano())
	subscriptionID := fmt.Sprintf("e2e-sub-%d", time.Now().UnixNano())

	return &gcpTestEnv{
		projectID:      projectID,
		topicID:        topicID,
		subscriptionID: subscriptionID,
	}
}

func (e *gcpTestEnv) createTransport(t *testing.T) *pubsub.PubSubTransport {
	t.Helper()

	ctx := context.Background()

	tr, err := pubsub.New(ctx, pubsub.Config{
		ProjectID:      e.projectID,
		TopicID:        e.topicID,
		SubscriptionID: e.subscriptionID,
		MaxMessages:    10,
	})
	if err != nil {
		t.Fatalf("create pubsub transport: %v", err)
	}

	e.transport = tr
	return tr
}

func (e *gcpTestEnv) cleanup(t *testing.T) {
	t.Helper()
	if e.transport != nil {
		e.transport.Close()
	}
}
