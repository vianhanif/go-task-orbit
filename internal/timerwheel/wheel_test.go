package timerwheel

import (
	"context"
	"testing"
	"time"
)

func TestInsertAndExpire(t *testing.T) {
	w := New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w.Insert("msg1", 500*time.Millisecond)
	w.Insert("msg2", 100*time.Millisecond)

	ch := w.Start(ctx)

	var received []string
	timeout := time.After(2 * time.Second)
	done := false

	for !done {
		select {
		case batch := <-ch:
			for _, item := range batch {
				received = append(received, item.(string))
			}
		case <-timeout:
			done = true
		}
	}

	if len(received) != 2 {
		t.Fatalf("expected 2 items, got %d: %v", len(received), received)
	}
}

func TestDrain(t *testing.T) {
	w := New()

	w.Insert("a", 10*time.Second)
	w.Insert("b", 20*time.Second)
	w.Insert("c", 30*time.Second)

	if w.Len() != 3 {
		t.Fatalf("expected 3 pending, got %d", w.Len())
	}

	drained := w.Drain()
	if len(drained) != 3 {
		t.Fatalf("expected 3 drained, got %d", len(drained))
	}

	if w.Len() != 0 {
		t.Errorf("expected 0 after drain, got %d", w.Len())
	}
}

func TestEmptyWheel(t *testing.T) {
	w := New()

	if w.Len() != 0 {
		t.Errorf("expected 0, got %d", w.Len())
	}

	drained := w.Drain()
	if len(drained) != 0 {
		t.Errorf("expected empty drain, got %d", len(drained))
	}
}

func TestClose(t *testing.T) {
	w := New()
	w.Close()

	if w.Len() != 0 {
		t.Errorf("expected 0 after close, got %d", w.Len())
	}

	w.Insert("x", time.Second)
	if w.Len() != 0 {
		t.Errorf("expected insert to be ignored after close")
	}
}
