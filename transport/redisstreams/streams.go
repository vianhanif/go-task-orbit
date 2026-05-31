package redisstreams

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	Addr           string
	StreamKey      string
	ConsumerGroup  string
	ConsumerName   string
	DLQStreamKey   string
	Password       string
	DB             int
	BlockTimeout   time.Duration
	BatchSize      int64
	IdleTimeout    time.Duration
	TopicAttribute string
}

func (c *Config) defaults() {
	if c.BlockTimeout == 0 {
		c.BlockTimeout = 2 * time.Second
	}
	if c.BatchSize == 0 {
		c.BatchSize = 10
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.TopicAttribute == "" {
		c.TopicAttribute = "X-Topic"
	}
	if c.ConsumerName == "" {
		host, _ := os.Hostname()
		c.ConsumerName = fmt.Sprintf("%s-%d", host, os.Getpid())
	}
}

type StreamsTransport struct {
	client       *redis.Client
	config       Config
	consumerName string
}

func New(cfg Config) *StreamsTransport {
	cfg.defaults()

	opts := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	return NewWithClient(redis.NewClient(opts), cfg)
}

func NewWithClient(client *redis.Client, cfg Config) *StreamsTransport {
	cfg.defaults()
	return &StreamsTransport{
		client:       client,
		config:       cfg,
		consumerName: cfg.ConsumerName,
	}
}

func (t *StreamsTransport) bootstrap(ctx context.Context) error {
	err := t.client.XGroupCreateMkStream(ctx, t.config.StreamKey, t.config.ConsumerGroup, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("redisstreams: XGroupCreateMkStream: %w", err)
	}
	return nil
}

func (t *StreamsTransport) recoverStale(ctx context.Context) error {
	pending, err := t.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream:   t.config.StreamKey,
		Group:    t.config.ConsumerGroup,
		Start:    "-",
		End:      "+",
		Count:    100,
		Consumer: t.consumerName,
	}).Result()
	if err != nil && !strings.Contains(err.Error(), "NOGROUP") {
		return err
	}

	for _, p := range pending {
		t.client.XAck(ctx, t.config.StreamKey, t.config.ConsumerGroup, p.ID)
	}

	claimed, err := t.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   t.config.StreamKey,
		Group:    t.config.ConsumerGroup,
		Consumer: t.consumerName,
		MinIdle:  t.config.IdleTimeout,
		Messages: []string{},
	}).Result()
	if err != nil {
		return nil
	}

	for _, msg := range claimed {
		t.client.XAck(ctx, t.config.StreamKey, t.config.ConsumerGroup, msg.ID)
	}
	return nil
}

func (t *StreamsTransport) Publish(ctx context.Context, msg ringq.Message) error {
	attrs := msg.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}
	attrs[t.config.TopicAttribute] = msg.Topic
	if msg.NotBefore > 0 {
		attrs["X-NotBefore"] = msg.NotBefore.String()
	}

	values := map[string]interface{}{
		"payload":    string(msg.Payload),
		"topic":      msg.Topic,
		"attributes": msg.Attributes,
	}
	return t.client.XAdd(ctx, &redis.XAddArgs{
		Stream: t.config.StreamKey,
		Values: values,
	}).Err()
}

func (t *StreamsTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
	if err := t.bootstrap(ctx); err != nil {
		return err
	}
	t.recoverStale(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		result, err := t.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    t.config.ConsumerGroup,
			Consumer: t.consumerName,
			Streams:  []string{t.config.StreamKey, ">"},
			Count:    t.config.BatchSize,
			Block:    t.config.BlockTimeout,
		}).Result()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			continue
		}

		for _, stream := range result {
			messages := make([]ringq.Message, len(stream.Messages))
			for i, xmsg := range stream.Messages {
				payload, _ := xmsg.Values["payload"].(string)
				topic, _ := xmsg.Values["topic"].(string)

				attrs := make(map[string]string)
				if rawAttrs, ok := xmsg.Values["attributes"].(map[interface{}]interface{}); ok {
					for k, v := range rawAttrs {
						attrs[fmt.Sprint(k)] = fmt.Sprint(v)
					}
				}

				var notBefore time.Duration
				if nb, ok := attrs["X-NotBefore"]; ok {
					notBefore, _ = time.ParseDuration(nb)
				}

				messages[i] = ringq.Message{
					ID:            xmsg.ID,
					Topic:         topic,
					Payload:       []byte(payload),
					Attributes:    attrs,
					NotBefore:     notBefore,
					ReceiptHandle: xmsg.ID,
				}
			}

			if err := handler(ctx, messages); err != nil {
				return err
			}
		}
	}
}

func (t *StreamsTransport) Ack(_ context.Context, messages []ringq.Message) error {
	ids := make([]string, len(messages))
	for i, m := range messages {
		ids[i] = m.ReceiptHandle
	}
	return t.client.XAck(context.Background(), t.config.StreamKey, t.config.ConsumerGroup, ids...).Err()
}

func (t *StreamsTransport) Nack(_ context.Context, msg ringq.Message, delay time.Duration) error {
	if delay > 0 {
		return t.client.XClaim(context.Background(), &redis.XClaimArgs{
			Stream:   t.config.StreamKey,
			Group:    t.config.ConsumerGroup,
			Consumer: t.consumerName,
			MinIdle:  delay,
			Messages: []string{msg.ReceiptHandle},
		}).Err()
	}
	return t.client.XAck(context.Background(), t.config.StreamKey, t.config.ConsumerGroup, msg.ReceiptHandle).Err()
}

func (t *StreamsTransport) SendToDLQ(_ context.Context, msg ringq.Message) error {
	if t.config.DLQStreamKey == "" {
		return nil
	}
	values := map[string]interface{}{
		"payload":    string(msg.Payload),
		"topic":      msg.Topic,
		"attributes": msg.Attributes,
		"dlq_reason": "max_retries_exceeded",
	}
	if err := t.client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: t.config.DLQStreamKey,
		Values: values,
	}).Err(); err != nil {
		return err
	}
	return t.client.XAck(context.Background(), t.config.StreamKey, t.config.ConsumerGroup, msg.ReceiptHandle).Err()
}

func (t *StreamsTransport) Close() error {
	return t.client.Close()
}
