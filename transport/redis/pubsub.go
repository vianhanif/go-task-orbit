package redis

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	Addr           string
	Channels       []string
	Password       string
	DB             int
	TopicAttribute string
}

func (c *Config) defaults() {
	if c.TopicAttribute == "" {
		c.TopicAttribute = "X-Topic"
	}
}

type PubSubTransport struct {
	client *redis.Client
	config Config
	pubsub *redis.PubSub
}

func New(cfg Config) *PubSubTransport {
	cfg.defaults()

	opts := &redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
	}
	client := redis.NewClient(opts)

	return &PubSubTransport{
		client: client,
		config: cfg,
	}
}

func NewWithClient(client *redis.Client, cfg Config) *PubSubTransport {
	cfg.defaults()
	return &PubSubTransport{
		client: client,
		config: cfg,
	}
}

func (t *PubSubTransport) Publish(ctx context.Context, msg ringq.Message) error {
	attrs := msg.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}
	attrs[t.config.TopicAttribute] = msg.Topic

	wire := wireMsg{Payload: msg.Payload, Attributes: attrs}
	data, err := json.Marshal(wire)
	if err != nil {
		return err
	}

	channel := t.config.Channels[0]
	return t.client.Publish(ctx, channel, data).Err()
}

type wireMsg struct {
	Payload    []byte            `json:"payload"`
	Attributes map[string]string `json:"attributes"`
}

func (t *PubSubTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
	t.pubsub = t.client.Subscribe(ctx, t.config.Channels...)
	defer t.pubsub.Close()

	ch := t.pubsub.Channel()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case redisMsg, ok := <-ch:
			if !ok {
				return nil
			}

			var wire wireMsg
			if err := json.Unmarshal([]byte(redisMsg.Payload), &wire); err != nil {
				continue
			}

			topic := ""
			if wire.Attributes != nil {
				topic = wire.Attributes[t.config.TopicAttribute]
			}

			ringqMsg := ringq.Message{
				ID:         redisMsg.Channel,
				Topic:      topic,
				Payload:    wire.Payload,
				Attributes: wire.Attributes,
			}

			if err := handler(ctx, []ringq.Message{ringqMsg}); err != nil {
				return err
			}
		}
	}
}

// Ack is a no-op. Redis Pub/Sub has no acknowledgment protocol.
// For durable message processing, use Redis Streams transport.
func (t *PubSubTransport) Ack(_ context.Context, _ []ringq.Message) error {
	return nil
}

// Nack is a no-op. Redis Pub/Sub cannot re-queue messages.
// For retry support, use Redis Streams transport.
func (t *PubSubTransport) Nack(_ context.Context, _ ringq.Message, _ time.Duration) error {
	return nil
}

// SendToDLQ is a no-op. Redis Pub/Sub has no dead letter concept.
// For DLQ support, use Redis Streams transport.
func (t *PubSubTransport) SendToDLQ(_ context.Context, _ ringq.Message) error {
	return nil
}

func (t *PubSubTransport) Close() error {
	if t.pubsub != nil {
		t.pubsub.Close()
	}
	return t.client.Close()
}
