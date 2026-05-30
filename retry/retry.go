package retry

import (
	"context"
	"fmt"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
	MaxRetries    int
	BaseDelay     time.Duration
	OnRetry       func(ctx context.Context, msg ringq.Message, attempt int)
	OnDLQ         func(ctx context.Context, msg ringq.Message, err error)
}

func (c *Config) defaults() {
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.BaseDelay == 0 {
		c.BaseDelay = 5 * time.Second
	}
}

type Coordinator struct {
	config   Config
}

func NewCoordinator(cfg Config) *Coordinator {
	cfg.defaults()
	return &Coordinator{
		config: cfg,
	}
}

type Outcome struct {
	Action  ringq.Action
	Message ringq.Message
	Delay   time.Duration
	Err     error
}

func (c *Coordinator) Handle(ctx context.Context, msg ringq.Message, result ringq.Result) Outcome {
	switch result.Action {
	case ringq.Ack:
		return Outcome{
			Action:  ringq.Ack,
			Message: msg,
		}

	case ringq.Retry:
		attempt := msg.Attempts + 1
		if attempt > c.config.MaxRetries {
			err := fmt.Errorf("retry: max retries (%d) exceeded: %w", c.config.MaxRetries, result.Err)
			if c.config.OnDLQ != nil {
				c.config.OnDLQ(ctx, msg, err)
			}
			return Outcome{
				Action:  ringq.DLQ,
				Message: msg,
				Err:     err,
			}
		}
		msg.Attempts = attempt
		if c.config.OnRetry != nil {
			c.config.OnRetry(ctx, msg, attempt)
		}
		return Outcome{
			Action:  ringq.Retry,
			Message: msg,
			Delay:   0,
		}

	case ringq.RetryWithDelay:
		attempt := msg.Attempts + 1
		if attempt > c.config.MaxRetries {
			err := fmt.Errorf("retry: max retries (%d) exceeded: %w", c.config.MaxRetries, result.Err)
			if c.config.OnDLQ != nil {
				c.config.OnDLQ(ctx, msg, err)
			}
			return Outcome{
				Action:  ringq.DLQ,
				Message: msg,
				Err:     err,
			}
		}
		msg.Attempts = attempt
		if c.config.OnRetry != nil {
			c.config.OnRetry(ctx, msg, attempt)
		}
		delay := result.Delay
		if delay <= 0 {
			delay = c.config.BaseDelay
		}
		return Outcome{
			Action:  ringq.RetryWithDelay,
			Message: msg,
			Delay:   delay,
		}

	case ringq.DLQ:
		if c.config.OnDLQ != nil {
			c.config.OnDLQ(ctx, msg, result.Err)
		}
		return Outcome{
			Action:  ringq.DLQ,
			Message: msg,
			Err:     result.Err,
		}

	default:
		return Outcome{
			Action:  ringq.DLQ,
			Message: msg,
			Err:     fmt.Errorf("retry: unknown action %v", result.Action),
		}
	}
}
