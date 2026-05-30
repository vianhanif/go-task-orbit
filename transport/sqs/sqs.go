package sqs

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	QueueURL          string
	DLQURL            string
	MaxMessages       int32
	WaitTime          int32
	VisibilityTimeout int32
	TopicAttribute    string
}

func (c *Config) defaults() {
	if c.MaxMessages == 0 {
		c.MaxMessages = 10
	}
	if c.WaitTime == 0 {
		c.WaitTime = 20
	}
	if c.VisibilityTimeout == 0 {
		c.VisibilityTimeout = 30
	}
}

type SQSTransport struct {
	client *sqs.Client
	config Config
	stopCh chan struct{}
}

func New(cfg Config) *SQSTransport {
	cfg.defaults()
	return &SQSTransport{
		config: cfg,
		stopCh: make(chan struct{}),
	}
}

func (t *SQSTransport) WithClient(client *sqs.Client) *SQSTransport {
	t.client = client
	return t
}

func (t *SQSTransport) ensureClient(ctx context.Context) error {
	if t.client != nil {
		return nil
	}
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("sqs: failed to load AWS config: %w", err)
	}
	t.client = sqs.NewFromConfig(awsCfg)
	return nil
}

func (t *SQSTransport) Publish(ctx context.Context, msg ringq.Message) error {
	if err := t.ensureClient(ctx); err != nil {
		return err
	}
	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(t.config.QueueURL),
		MessageBody:       aws.String(string(msg.Payload)),
		MessageAttributes: toSQSAttributes(msg.Attributes),
	}
	_, err := t.client.SendMessage(ctx, input)
	return err
}

func (t *SQSTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
	if err := t.ensureClient(ctx); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.stopCh:
			return nil
		default:
		}

		output, err := t.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(t.config.QueueURL),
			MaxNumberOfMessages: t.config.MaxMessages,
			WaitTimeSeconds:     t.config.WaitTime,
			VisibilityTimeout:   t.config.VisibilityTimeout,
			AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
			MessageAttributeNames: []string{"All"},
		})
		if err != nil {
			return fmt.Errorf("sqs: receive message: %w", err)
		}
		if len(output.Messages) == 0 {
			continue
		}

		messages := make([]ringq.Message, len(output.Messages))
		for i, m := range output.Messages {
			messages[i] = t.fromSQSMessage(m)
		}

		if err := handler(ctx, messages); err != nil {
			return err
		}
	}
}

func (t *SQSTransport) Ack(ctx context.Context, messages []ringq.Message) error {
	if err := t.ensureClient(ctx); err != nil {
		return err
	}
	entries := make([]types.DeleteMessageBatchRequestEntry, len(messages))
	for i, m := range messages {
		entries[i] = types.DeleteMessageBatchRequestEntry{
			Id:            aws.String(m.ID),
			ReceiptHandle: aws.String(m.ReceiptHandle),
		}
	}
	_, err := t.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(t.config.QueueURL),
		Entries:  entries,
	})
	return err
}

func (t *SQSTransport) Nack(ctx context.Context, message ringq.Message, delay time.Duration) error {
	if err := t.ensureClient(ctx); err != nil {
		return err
	}
	seconds := int32(delay.Seconds())
	if seconds < 0 {
		seconds = 0
	}
	_, err := t.client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
		QueueUrl:          aws.String(t.config.QueueURL),
		ReceiptHandle:     aws.String(message.ReceiptHandle),
		VisibilityTimeout: seconds,
	})
	return err
}

func (t *SQSTransport) SendToDLQ(ctx context.Context, message ringq.Message) error {
	if t.config.DLQURL == "" {
		return fmt.Errorf("sqs: no DLQ URL configured")
	}
	if err := t.ensureClient(ctx); err != nil {
		return err
	}
	input := &sqs.SendMessageInput{
		QueueUrl:          aws.String(t.config.DLQURL),
		MessageBody:       aws.String(string(message.Payload)),
		MessageAttributes: toSQSAttributes(message.Attributes),
	}
	_, err := t.client.SendMessage(ctx, input)
	return err
}

func (t *SQSTransport) Close() error {
	close(t.stopCh)
	return nil
}

func toSQSAttributes(attrs map[string]string) map[string]types.MessageAttributeValue {
	if attrs == nil {
		return nil
	}
	result := make(map[string]types.MessageAttributeValue, len(attrs))
	for k, v := range attrs {
		result[k] = types.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(v),
		}
	}
	return result
}

func fromSQSAttributes(attrs map[string]types.MessageAttributeValue) map[string]string {
	if attrs == nil {
		return nil
	}
	result := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if v.StringValue != nil {
			result[k] = *v.StringValue
		}
	}
	return result
}

func (t *SQSTransport) fromSQSMessage(m types.Message) ringq.Message {
	attrs := fromSQSAttributes(m.MessageAttributes)
	topic := ""
	if t.config.TopicAttribute != "" {
		topic = attrs[t.config.TopicAttribute]
	}
	return ringq.Message{
		ID:            *m.MessageId,
		Topic:         topic,
		Payload:       []byte(*m.Body),
		ReceiptHandle: *m.ReceiptHandle,
		Attributes:    attrs,
	}
}
