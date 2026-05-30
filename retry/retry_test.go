package retry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

func TestAck(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 3})
	msg := ringq.Message{ID: "1"}
	result := ringq.Result{Action: ringq.Ack}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.Ack {
		t.Errorf("expected Ack, got %v", outcome.Action)
	}
}

func TestRetryUnderLimit(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 3})
	msg := ringq.Message{ID: "1"}
	result := ringq.Result{Action: ringq.Retry, Err: errors.New("temp error")}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.Retry {
		t.Errorf("expected Retry, got %v", outcome.Action)
	}
	if outcome.Message.Attempts != 1 {
		t.Errorf("expected attempts 1, got %d", outcome.Message.Attempts)
	}
}

func TestRetryExceedsLimit(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 2})
	msg := ringq.Message{ID: "1", Attempts: 2}
	result := ringq.Result{Action: ringq.Retry, Err: errors.New("persistent error")}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.DLQ {
		t.Errorf("expected DLQ after max retries, got %v", outcome.Action)
	}
}

func TestRetryWithDelay(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 3})
	msg := ringq.Message{ID: "1"}
	result := ringq.Result{Action: ringq.RetryWithDelay, Delay: 30 * time.Second}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.RetryWithDelay {
		t.Errorf("expected RetryWithDelay, got %v", outcome.Action)
	}
	if outcome.Delay != 30*time.Second {
		t.Errorf("expected delay 30s, got %v", outcome.Delay)
	}
}

func TestRetryWithDelayDefaultDelay(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 3, BaseDelay: 10 * time.Second})
	msg := ringq.Message{ID: "1"}
	result := ringq.Result{Action: ringq.RetryWithDelay}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Delay != 10*time.Second {
		t.Errorf("expected default delay 10s, got %v", outcome.Delay)
	}
}

func TestDLQ(t *testing.T) {
	c := NewCoordinator(Config{MaxRetries: 3})
	msg := ringq.Message{ID: "1"}
	sentinel := errors.New("unrecoverable")
	result := ringq.Result{Action: ringq.DLQ, Err: sentinel}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.DLQ {
		t.Errorf("expected DLQ, got %v", outcome.Action)
	}
	if !errors.Is(outcome.Err, sentinel) {
		t.Errorf("expected sentinel error, got %v", outcome.Err)
	}
}

func TestOnRetryHook(t *testing.T) {
	var hooked bool
	c := NewCoordinator(Config{
		MaxRetries: 3,
		OnRetry: func(_ context.Context, _ ringq.Message, attempt int) {
			hooked = true
		},
	})
	msg := ringq.Message{ID: "1"}
	result := ringq.Result{Action: ringq.Retry, Err: errors.New("err")}

	c.Handle(context.Background(), msg, result)
	if !hooked {
		t.Error("expected OnRetry hook to fire")
	}
}

func TestOnDLQHook(t *testing.T) {
	var hooked bool
	c := NewCoordinator(Config{
		MaxRetries: 3,
		OnDLQ: func(_ context.Context, _ ringq.Message, _ error) {
			hooked = true
		},
	})
	msg := ringq.Message{ID: "1", Attempts: 3}
	result := ringq.Result{Action: ringq.Retry, Err: errors.New("err")}

	outcome := c.Handle(context.Background(), msg, result)
	if outcome.Action != ringq.DLQ {
		t.Fatal("expected DLQ")
	}
	if !hooked {
		t.Error("expected OnDLQ hook to fire")
	}
}
