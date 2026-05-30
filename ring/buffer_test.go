package ring

import (
	"testing"
)

func TestBasicEnqueueDequeue(t *testing.T) {
	b := New(4, DropOldest)
	for i := 0; i < b.Cap(); i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != b.Cap() {
		t.Fatalf("expected len %d, got %d", b.Cap(), b.Len())
	}
	for i := 0; i < b.Cap(); i++ {
		item, ok := b.Dequeue()
		if !ok {
			t.Fatalf("expected item %d", i)
		}
		if item.(int) != i {
			t.Errorf("expected %d, got %d", i, item.(int))
		}
	}
	if _, ok := b.Dequeue(); ok {
		t.Error("expected empty buffer")
	}
}

func TestDropOldest(t *testing.T) {
	b := New(3, DropOldest)
	cap := b.Cap()
	for i := 0; i < cap+1; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != cap {
		t.Fatalf("expected len %d, got %d", cap, b.Len())
	}
	item, ok := b.Dequeue()
	if !ok {
		t.Fatal("expected an item")
	}
	if item.(int) != 1 {
		t.Errorf("expected first item to be 1 (0 dropped), got %d", item.(int))
	}
}

func TestDropNewest(t *testing.T) {
	b := New(3, DropNewest)
	cap := b.Cap()
	for i := 0; i < cap+1; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != cap {
		t.Fatalf("expected len %d, got %d", cap, b.Len())
	}
	item, ok := b.Dequeue()
	if !ok {
		t.Fatal("expected an item")
	}
	if item.(int) != 0 {
		t.Errorf("expected first item 0, got %d", item.(int))
	}
}

func TestReject(t *testing.T) {
	b := New(2, Reject)
	cap := b.Cap()
	for i := 0; i < cap; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if err := b.Enqueue(cap); err != ErrBufferFull {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
}

func TestCapacity(t *testing.T) {
	b := New(64, Block)
	if b.Cap() != 64 {
		t.Errorf("expected cap 64, got %d", b.Cap())
	}
}

func TestCloseWakesBlocked(t *testing.T) {
	b := New(1, Block)
	b.Enqueue(1)

	done := make(chan bool)
	go func() {
		err := b.Enqueue(2)
		if err == nil {
			t.Error("expected error on closed buffer")
		}
		done <- true
	}()

	b.Close()
	<-done
}
