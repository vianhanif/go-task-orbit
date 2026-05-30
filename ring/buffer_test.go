package ring

import (
	"testing"
)

func TestBasicEnqueueDequeue(t *testing.T) {
	b := New(4, DropOldest)
	for i := 0; i < 4; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != 4 {
		t.Fatalf("expected len 4, got %d", b.Len())
	}
	for i := 0; i < 4; i++ {
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
	for i := 0; i < 5; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != 3 {
		t.Fatalf("expected len 3, got %d", b.Len())
	}
	// oldest 0,1 should be dropped; 2,3,4 remain
	for i := 2; i <= 4; i++ {
		item, ok := b.Dequeue()
		if !ok {
			t.Fatalf("expected item %d", i)
		}
		if item.(int) != i {
			t.Errorf("expected %d, got %d", i, item.(int))
		}
	}
}

func TestDropNewest(t *testing.T) {
	b := New(3, DropNewest)
	for i := 0; i < 5; i++ {
		if err := b.Enqueue(i); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	if b.Len() != 3 {
		t.Fatalf("expected len 3, got %d", b.Len())
	}
	// newest 3,4 should be dropped; 0,1,2 remain
	for i := 0; i <= 2; i++ {
		item, ok := b.Dequeue()
		if !ok {
			t.Fatalf("expected item %d", i)
		}
		if item.(int) != i {
			t.Errorf("expected %d, got %d", i, item.(int))
		}
	}
}

func TestReject(t *testing.T) {
	b := New(2, Reject)
	if err := b.Enqueue(1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := b.Enqueue(2); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := b.Enqueue(3); err != ErrBufferFull {
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
