package idempotency

import (
	"context"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestMemoryStoreExists(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	ctx := context.Background()

	exists, err := s.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected key1 to not exist")
	}

	if err := s.Mark(ctx, "key1", time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exists, err = s.Exists(ctx, "key1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !exists {
		t.Error("expected key1 to exist after Mark")
	}
}

func TestMemoryStoreTTL(t *testing.T) {
	s := NewMemoryStore()
	defer s.Close()

	ctx := context.Background()
	s.Mark(ctx, "expired", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	exists, err := s.Exists(ctx, "expired")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exists {
		t.Error("expected expired key to be evicted")
	}
}

func TestFilterPassThrough(t *testing.T) {
	f := NewFilter(Config{
		Store:        NewMemoryStore(),
		AttributeKey: "IdempotencyKey",
		TTL:          time.Minute,
	})

	ctx := context.Background()
	msgs := []ringq.Message{
		{ID: "1", Attributes: map[string]string{"IdempotencyKey": "first"}},
		{ID: "2", Attributes: map[string]string{"IdempotencyKey": "second"}},
		{ID: "3", Attributes: nil},
	}

	filtered, _ := f.Filter(ctx, msgs)
	if len(filtered) != 3 {
		t.Fatalf("expected 3 messages on first pass, got %d", len(filtered))
	}

	for _, m := range filtered {
		f.Mark(ctx, m)
	}

	filtered, _ = f.Filter(ctx, msgs)
	if len(filtered) != 1 {
		t.Fatalf("expected 1 message (no-key) on second pass, got %d", len(filtered))
	}
	if filtered[0].ID != "3" {
		t.Errorf("expected msg ID 3 (no key), got %s", filtered[0].ID)
	}
}

func TestFilterDeduplicates(t *testing.T) {
	f := NewFilter(Config{
		Store:        NewMemoryStore(),
		AttributeKey: "IdempotencyKey",
		TTL:          time.Minute,
	})

	ctx := context.Background()
	msgs := []ringq.Message{
		{ID: "1", Attributes: map[string]string{"IdempotencyKey": "same"}},
	}

	first, _ := f.Filter(ctx, msgs)
	if len(first) != 1 {
		t.Fatal("expected first pass to allow message through")
	}

	f.Mark(ctx, first[0])

	second, duplicates := f.Filter(ctx, msgs)
	if len(second) != 0 {
		t.Fatal("expected second pass to filter duplicate")
	}
	if len(duplicates) != 1 {
		t.Fatal("expected duplicate returned")
	}
}

func TestFilterNoStore(t *testing.T) {
	f := NewFilter(Config{})
	msgs := []ringq.Message{{ID: "1", Attributes: map[string]string{"k": "v"}}}

	filtered, _ := f.Filter(context.Background(), msgs)
	if len(filtered) != 1 {
		t.Errorf("expected all messages through with no store, got %d", len(filtered))
	}
}

func TestFilterOnDuplicateHook(t *testing.T) {
	var hooked bool
	f := NewFilter(Config{
		Store:        NewMemoryStore(),
		AttributeKey: "IdempotencyKey",
		TTL:          time.Minute,
		OnDuplicate: func(_ context.Context, key string) {
			hooked = true
		},
	})

	ctx := context.Background()
	msg := ringq.Message{ID: "1", Attributes: map[string]string{"IdempotencyKey": "k"}}

	f.Filter(ctx, []ringq.Message{msg})
	f.Mark(ctx, msg)
	f.Filter(ctx, []ringq.Message{msg})

	if !hooked {
		t.Error("expected OnDuplicate hook to fire")
	}
}
