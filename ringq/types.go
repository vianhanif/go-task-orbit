package ringq

import (
	"context"
	"time"
)

type Action int

const (
	Ack            Action = iota
	Retry
	RetryWithDelay
	DLQ
)

type Result struct {
	Action Action
	Delay  time.Duration
	Err    error
}

type Handler[T any] func(ctx context.Context, msg T) Result

type Message struct {
	ID            string
	Topic         string
	Payload       []byte
	Attributes    map[string]string
	ReceiptHandle string
	Attempts      int
	NotBefore     time.Duration
}

type IdemStore interface {
	Exists(ctx context.Context, key string) (bool, error)
	Mark(ctx context.Context, key string, ttl time.Duration) error
	Close() error
}

type RetryOutcome struct {
	Action  Action
	Message Message
	Delay   time.Duration
	Err     error
}

type RetryCoordinator interface {
	Handle(ctx context.Context, msg Message, result Result) RetryOutcome
}
