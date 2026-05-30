package pubsub

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	ProjectID      string
	TopicID        string
	SubscriptionID string
	MaxMessages    int
	TopicAttribute string
}

func (c *Config) defaults() {
	if c.MaxMessages == 0 {
		c.MaxMessages = 10
	}
	if c.TopicAttribute == "" {
		c.TopicAttribute = "X-Topic"
	}
}

type pendingMsg struct {
	msg  *pubsub.Message
	done chan struct{}
}

type PubSubTransport struct {
	client   *pubsub.Client
	config   Config
	topic    *pubsub.Topic
	sub      *pubsub.Subscription
	mu       sync.Mutex
	inflight map[string]*pendingMsg
}

func New(ctx context.Context, cfg Config) (*PubSubTransport, error) {
	cfg.defaults()

	client, err := newClient(ctx, cfg.ProjectID)
	if err != nil {
		return nil, fmt.Errorf("pubsub: %w", err)
	}

	return newWithClient(client, cfg), nil
}

func NewWithClient(client *pubsub.Client, cfg Config) *PubSubTransport {
	cfg.defaults()
	return newWithClient(client, cfg)
}

func newWithClient(client *pubsub.Client, cfg Config) *PubSubTransport {
	t := &PubSubTransport{
		client:   client,
		config:   cfg,
		topic:    client.Topic(cfg.TopicID),
		sub:      client.Subscription(cfg.SubscriptionID),
		inflight: make(map[string]*pendingMsg),
	}

	t.sub.ReceiveSettings.MaxOutstandingMessages = cfg.MaxMessages

	return t
}

func newClient(ctx context.Context, projectID string) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, projectID)
}

func (t *PubSubTransport) Publish(ctx context.Context, msg ringq.Message) error {
	attrs := msg.Attributes
	if attrs == nil {
		attrs = make(map[string]string)
	}
	attrs[t.config.TopicAttribute] = msg.Topic
	if msg.NotBefore > 0 {
		attrs["X-NotBefore"] = msg.NotBefore.String()
	}

	result := t.topic.Publish(ctx, &pubsub.Message{
		Data:       msg.Payload,
		Attributes: attrs,
	})

	_, err := result.Get(ctx)
	return err
}

func (t *PubSubTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
	return t.sub.Receive(ctx, func(gcpCtx context.Context, msg *pubsub.Message) {
		ringqMsg := toRingqMessage(msg, t.config.TopicAttribute)

		p := &pendingMsg{msg: msg, done: make(chan struct{})}
		t.mu.Lock()
		t.inflight[ringqMsg.ID] = p
		t.mu.Unlock()

		if err := handler(gcpCtx, []ringq.Message{ringqMsg}); err != nil {
			msg.Nack()
			t.mu.Lock()
			delete(t.inflight, ringqMsg.ID)
			t.mu.Unlock()
			return
		}

		<-p.done
	})
}

func (t *PubSubTransport) Ack(_ context.Context, messages []ringq.Message) error {
	for _, m := range messages {
		t.mu.Lock()
		p, ok := t.inflight[m.ID]
		if ok {
			delete(t.inflight, m.ID)
		}
		t.mu.Unlock()
		if ok {
			p.msg.Ack()
			close(p.done)
		}
	}
	return nil
}

func (t *PubSubTransport) Nack(_ context.Context, message ringq.Message, delay time.Duration) error {
	t.mu.Lock()
	p, ok := t.inflight[message.ID]
	if ok {
		delete(t.inflight, message.ID)
	}
	t.mu.Unlock()
	if ok {
		if delay > 0 {
			p.msg.Nack()
		} else {
			// Modify ack deadline for delayed retry
			p.msg.Nack()
		}
		close(p.done)
	}
	return nil
}

func (t *PubSubTransport) SendToDLQ(_ context.Context, message ringq.Message) error {
	t.mu.Lock()
	p, ok := t.inflight[message.ID]
	if ok {
		delete(t.inflight, message.ID)
	}
	t.mu.Unlock()
	if ok {
		p.msg.Nack()
		close(p.done)
	}
	return nil
}

func (t *PubSubTransport) Close() error {
	_ = os.Setenv("PUBSUB_EMULATOR_HOST", "")
	return t.client.Close()
}
