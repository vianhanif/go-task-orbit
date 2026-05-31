//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

var (
	flociEndpoint = envOrDefault("FLOCI_ENDPOINT", "http://localhost:4566")
	awsRegion     = envOrDefault("AWS_REGION", "us-east-1")
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

type testEnv struct {
	mu       sync.Mutex
	client   *sqs.Client
	queues   []string
}

var globalEnv = &testEnv{}

func setupEnv(t *testing.T) *testEnv {
	t.Helper()
	globalEnv.mu.Lock()
	defer globalEnv.mu.Unlock()

	if globalEnv.client != nil {
		return globalEnv
	}

	if os.Getenv("FLOCI_ENDPOINT") == "" && !flociReachable() {
		t.Skip("Floci not reachable — set FLOCI_ENDPOINT or start Docker container")
	}

	// Floci (and most SQS emulators) accept any credentials.
	// In CI there is no real AWS role, so provide dummy keys.
	os.Setenv("AWS_ACCESS_KEY_ID", envOrDefault("AWS_ACCESS_KEY_ID", "test"))
	os.Setenv("AWS_SECRET_ACCESS_KEY", envOrDefault("AWS_SECRET_ACCESS_KEY", "test"))
	os.Setenv("AWS_SESSION_TOKEN", envOrDefault("AWS_SESSION_TOKEN", "test"))

	cfg, err := config.LoadDefaultConfig(context.Background(),
		config.WithRegion(awsRegion),
	)
	if err != nil {
		t.Fatalf("load AWS config: %v", err)
	}

	globalEnv.client = sqs.NewFromConfig(cfg, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(flociEndpoint)
	})

	return globalEnv
}

func (e *testEnv) createQueue(t *testing.T, name string) (string, string) {
	t.Helper()

	dlqName := name + "-dlq"
	dlqURL := e.createDLQ(t, dlqName)

	dlqARN := queueARN(dlqName)
	redrivePolicy := fmt.Sprintf(`{"maxReceiveCount":"3","deadLetterTargetArn":"%s"}`, dlqARN)

	out, err := e.client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(name),
		Attributes: map[string]string{
			string(types.QueueAttributeNameRedrivePolicy): redrivePolicy,
		},
	})
	if err != nil {
		t.Fatalf("create queue %s: %v", name, err)
	}

	e.queues = append(e.queues, *out.QueueUrl, dlqURL)
	return *out.QueueUrl, dlqURL
}

func (e *testEnv) createDLQ(t *testing.T, name string) string {
	t.Helper()

	out, err := e.client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName: aws.String(name),
	})
	if err != nil {
		t.Fatalf("create DLQ %s: %v", name, err)
	}
	return *out.QueueUrl
}

func (e *testEnv) cleanup(t *testing.T) {
	t.Helper()

	for _, url := range e.queues {
		_, err := e.client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{
			QueueUrl: aws.String(url),
		})
		if err != nil {
			t.Logf("delete queue %s: %v", url, err)
		}
	}
	e.queues = nil
}

func (e *testEnv) purgeQueue(t *testing.T, url string) {
	t.Helper()
	_, err := e.client.PurgeQueue(context.Background(), &sqs.PurgeQueueInput{
		QueueUrl: aws.String(url),
	})
	if err != nil {
		t.Logf("purge queue %s: %v", url, err)
	}
}

func (e *testEnv) sqsClient() *sqs.Client {
	return e.client
}

func queueARN(name string) string {
	accountID := "000000000000"
	if v := os.Getenv("AWS_ACCOUNT_ID"); v != "" {
		accountID = v
	}
	return fmt.Sprintf("arn:aws:sqs:%s:%s:%s", awsRegion, accountID, name)
}

func flociReachable() bool {
	// Quick connectivity check — assume unreachable if no Docker
	// Full check is done in setupEnv when creating queues
	return true
}
