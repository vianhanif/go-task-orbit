package memory

import (
	"context"
	"sync"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Transport struct {
	mu       sync.Mutex
	cond     *sync.Cond
	messages []ringq.Message
	closed   bool
}

func New() *Transport {
	t := &Transport{}
	t.cond = sync.NewCond(&t.mu)
	return t
}

func (t *Transport) Publish(_ context.Context, msg ringq.Message) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.messages = append(t.messages, msg)
	t.cond.Signal()
	return nil
}

func (t *Transport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		t.mu.Lock()
		for len(t.messages) == 0 && !t.closed {
			t.cond.Wait()

			select {
			case <-ctx.Done():
				t.mu.Unlock()
				return ctx.Err()
			default:
			}
		}
		if t.closed && len(t.messages) == 0 {
			t.mu.Unlock()
			return nil
		}

		batch := t.messages
		t.messages = nil
		t.mu.Unlock()

		if handler != nil {
			if err := handler(ctx, batch); err != nil {
				return err
			}
		}
	}
}

func (t *Transport) Ack(_ context.Context, _ []ringq.Message) error {
	return nil
}

func (t *Transport) Nack(_ context.Context, _ ringq.Message, _ time.Duration) error {
	return nil
}

func (t *Transport) SendToDLQ(_ context.Context, _ ringq.Message) error {
	return nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	t.closed = true
	t.cond.Broadcast()
	t.mu.Unlock()
	return nil
}
