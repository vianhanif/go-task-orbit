package ringq

import (
	"context"
	"time"
)

type Hooks struct {
	OnReceive   func(ctx context.Context, count int)
	OnDispatch  func(ctx context.Context, topic string)
	OnComplete  func(ctx context.Context, topic string, dur time.Duration)
	OnError     func(ctx context.Context, topic string, err error)
	OnRetry     func(ctx context.Context, topic string, msg Message, attempt int)
	OnDuplicate func(ctx context.Context, key string)
}
