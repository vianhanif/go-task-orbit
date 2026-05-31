# Retries & DLQ

go-task-orbit provides a built-in retry engine with exponential backoff and dead letter queue routing.

## Result Actions

Handlers return a `ringq.Result` with one of four actions:

| Action | Behavior | When to use |
|---|---|---|
| `Ack` | Message acknowledged and deleted | Handler succeeded |
| `Retry` | Re-enqueued into ring buffer immediately | Transient failure, retry now |
| `RetryWithDelay` | Returned to transport with delay | Transient failure, retry later |
| `DLQ` | Routed to dead letter queue | Permanent failure, unrecoverable |

## Exponential Backoff (Default)

By default, retries use exponential backoff:

```
delay = min(baseDelay × 2^(attempt−1), 5 minutes)
```

| Attempt | Delay |
|---|---|
| 1 | immediate |
| 2 | 1s |
| 3 | 2s |
| 4 | 4s |
| 5 | 8s |
| ... | ... |
| 10 | 256s (capped at 300s) |
| 11 | DLQ |

Defaults: max 10 retries, 1s base delay. Configure via `HandleWithRetry`:

```go
pipeline.HandleWithRetry("topic", handler, maxRetries, baseDelay, coordinator)
```

## Transport-Specific Behavior

### SQS

- `Retry`: message re-enters ring buffer locally (no transport call)
- `RetryWithDelay`: calls `ChangeMessageVisibility` with calculated delay
- `DLQ`: publishes to separate DLQ queue, then acks original

### Pub/Sub

- `Retry`: message re-enters ring buffer locally
- `RetryWithDelay`: calls `msg.Nack()` — Pub/Sub redelivers after ack deadline
- `DLQ`: calls `msg.Nack()` — relies on subscription-level DLQ topic policy

### In-Memory

- `Retry`: message re-enters ring buffer locally
- `RetryWithDelay`: message re-published to simulate re-delivery
- `DLQ`: no-op

## Retry State

Retry state (`Attempts` count) is **in-memory** on `ringq.Message`. For SQS and Pub/Sub, transport redelivery resets the counter (the message is a new instance). The idempotency layer prevents duplicate side effects but does not prevent re-execution.

> **Best practice:** handlers should be idempotent regardless of retry behavior.

## Dead Letter Queue

### SQS DLQ

- Separate SQS queue URL configured in transport
- Messages published to DLQ queue with original payload + attributes
- Original message deleted from main queue after DLQ publish succeeds

### Pub/Sub DLQ

- Relies on subscription-level dead letter topic policy
- After `Nack()` and max delivery attempts, Pub/Sub automatically routes to DLQ topic
- Library does not manage DLQ topic creation — must be pre-configured on subscription
