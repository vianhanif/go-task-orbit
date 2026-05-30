package idempotency

import (
	"context"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Filter struct {
	store        ringq.IdemStore
	attributeKey string
	ttl          time.Duration
	onDuplicate  func(ctx context.Context, key string)
}

type Config struct {
	Store        ringq.IdemStore
	AttributeKey string
	TTL          time.Duration
	OnDuplicate  func(ctx context.Context, key string)
}

func NewFilter(cfg Config) *Filter {
	key := cfg.AttributeKey
	if key == "" {
		key = "IdempotencyKey"
	}
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return &Filter{
		store:        cfg.Store,
		attributeKey: key,
		ttl:          ttl,
		onDuplicate:  cfg.OnDuplicate,
	}
}

func (f *Filter) Filter(ctx context.Context, messages []ringq.Message) (kept []ringq.Message, duplicates []ringq.Message) {
	if f.store == nil {
		return messages, nil
	}

	kept = make([]ringq.Message, 0, len(messages))
	for _, msg := range messages {
		key, ok := msg.Attributes[f.attributeKey]
		if !ok {
			kept = append(kept, msg)
			continue
		}

		exists, err := f.store.Exists(ctx, key)
		if err != nil || exists {
			if f.onDuplicate != nil {
				f.onDuplicate(ctx, key)
			}
			duplicates = append(duplicates, msg)
			continue
		}

		kept = append(kept, msg)
	}
	return kept, duplicates
}

func (f *Filter) Mark(ctx context.Context, msg ringq.Message) error {
	if f.store == nil {
		return nil
	}
	key, ok := msg.Attributes[f.attributeKey]
	if !ok {
		return nil
	}
	return f.store.Mark(ctx, key, f.ttl)
}

func (f *Filter) Close() error {
	if f.store != nil {
		return f.store.Close()
	}
	return nil
}
