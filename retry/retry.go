package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	MaxRetries int
	BaseDelay  time.Duration
	OnRetry    func(ctx context.Context, msg ringq.Message, attempt int)
	OnDLQ      func(ctx context.Context, msg ringq.Message, err error)
}

func (c *Config) Defaults() {
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.BaseDelay == 0 {
		c.BaseDelay = 5 * time.Second
	}
}

type Coordinator struct {
	config Config
}

func NewCoordinator(cfg Config) *Coordinator {
	cfg.Defaults()
	return &Coordinator{config: cfg}
}

func (c *Coordinator) Handle(ctx context.Context, msg ringq.Message, result ringq.Result) ringq.RetryOutcome {
	switch result.Action {
	case ringq.Ack:
		return ringq.RetryOutcome{Action: ringq.Ack, Message: msg}

	case ringq.Retry:
		attempt := msg.Attempts + 1
		if attempt > c.config.MaxRetries {
			err := fmt.Errorf("retry: max retries (%d) exceeded: %w", c.config.MaxRetries, result.Err)
			if c.config.OnDLQ != nil {
				c.config.OnDLQ(ctx, msg, err)
			}
			return ringq.RetryOutcome{Action: ringq.DLQ, Message: msg, Err: err}
		}
		msg.Attempts = attempt
		if c.config.OnRetry != nil {
			c.config.OnRetry(ctx, msg, attempt)
		}
		return ringq.RetryOutcome{Action: ringq.Retry, Message: msg}

	case ringq.RetryWithDelay:
		attempt := msg.Attempts + 1
		if attempt > c.config.MaxRetries {
			err := fmt.Errorf("retry: max retries (%d) exceeded: %w", c.config.MaxRetries, result.Err)
			if c.config.OnDLQ != nil {
				c.config.OnDLQ(ctx, msg, err)
			}
			return ringq.RetryOutcome{Action: ringq.DLQ, Message: msg, Err: err}
		}
		msg.Attempts = attempt
		if c.config.OnRetry != nil {
			c.config.OnRetry(ctx, msg, attempt)
		}
		delay := result.Delay
		if delay <= 0 {
			delay = c.config.BaseDelay
		}
		return ringq.RetryOutcome{Action: ringq.RetryWithDelay, Message: msg, Delay: delay}

	case ringq.DLQ:
		if c.config.OnDLQ != nil {
			c.config.OnDLQ(ctx, msg, result.Err)
		}
		return ringq.RetryOutcome{Action: ringq.DLQ, Message: msg, Err: result.Err}

	default:
		return ringq.RetryOutcome{Action: ringq.DLQ, Message: msg, Err: fmt.Errorf("retry: unknown action %v", result.Action)}
	}
}
