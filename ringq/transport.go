package ringq

import (
	"context"
	"time"
)

type ConsumeHandler func(ctx context.Context, messages []Message) error

type Transport interface {
	Publish(ctx context.Context, msg Message) error
	Subscribe(ctx context.Context, handler ConsumeHandler) error
	Ack(ctx context.Context, messages []Message) error
	Nack(ctx context.Context, message Message, delay time.Duration) error
	SendToDLQ(ctx context.Context, message Message) error
	Close() error
}
