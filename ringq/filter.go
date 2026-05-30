package ringq

import (
	"context"
	"time"
)

type idemFilter struct {
	store        IdemStore
	attributeKey string
	ttl          time.Duration
	onDuplicate  func(ctx context.Context, key string)
}

func newIdemFilter(cfg IdempotencyConfig, onDuplicate func(ctx context.Context, key string)) *idemFilter {
	key := cfg.AttributeKey
	if key == "" {
		key = "IdempotencyKey"
	}
	ttl := cfg.TTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	return &idemFilter{
		store:        cfg.Store,
		attributeKey: key,
		ttl:          ttl,
		onDuplicate:  onDuplicate,
	}
}

func (f *idemFilter) filterDuplicates(ctx context.Context, messages []Message) (kept []Message, duplicates []Message) {
	if f.store == nil {
		return messages, nil
	}
	kept = make([]Message, 0, len(messages))
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

func (f *idemFilter) mark(ctx context.Context, msg Message) error {
	if f.store == nil {
		return nil
	}
	key, ok := msg.Attributes[f.attributeKey]
	if !ok {
		return nil
	}
	return f.store.Mark(ctx, key, f.ttl)
}

func (f *idemFilter) close() error {
	if f.store != nil {
		return f.store.Close()
	}
	return nil
}
