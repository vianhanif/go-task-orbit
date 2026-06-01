# Transports

go-task-orbit supports pluggable transport backends. Each transport implements `ringq.Transport`:

```go
type Transport interface {
    Publish(ctx context.Context, msg Message) error
    Subscribe(ctx context.Context, handler ConsumeHandler) error
    Ack(ctx context.Context, messages []Message) error
    Nack(ctx context.Context, message Message, delay time.Duration) error
    SendToDLQ(ctx context.Context, message Message) error
    Close() error
}
```

## Supported Transports

| Transport | Protocol | Durability | Ack | Retry | DLQ |
|---|---|---|---|---|---|
| **SQS** | HTTP | Persistent | `DeleteMessageBatch` | `ChangeMessageVisibility` | Separate DLQ queue |
| **Pub/Sub** | gRPC | Persistent | `msg.Ack()` | `msg.Nack()` | DLQ topic (subscription policy) |
| **In-Memory** | In-process | None | No-op | Re-publish | No-op |
| **Redis Pub/Sub** | TCP (RESP) | None (at-most-once) | No-op | No-op | No-op |
| **Redis Streams** | TCP (RESP) | Persistent | XACK | XCLAIM | Separate DLQ stream |

## Transport Selection

| Use case | Recommended transport |
|---|---|
| AWS production workloads | SQS |
| GCP production workloads | Pub/Sub |
| Redis shops migrating from go-workers | Redis Streams (supported) |
| Real-time broadcast (notifications, live dashboards) | Redis Pub/Sub |
| Local development and testing | In-Memory |

## SQS Details

- Batch receive up to 10 messages per `ReceiveMessage` call
- `DeleteMessageBatch` for acknowledged messages
- `ChangeMessageVisibility` for delayed retry
- DLQ via separate SQS queue URL
- `BaseEndpoint` config for emulator support (Floci, LocalStack)
- KEDA-compatible for autoscaling based on queue depth

## Pub/Sub Details

- gRPC streaming pull via `subscription.Receive()`
- Individual `msg.Ack()` per message (no batch ack)
- `msg.Nack()` for retry — redelivers after ack deadline
- DLQ via subscription-level dead letter topic policy
- `PUBSUB_EMULATOR_HOST` for Google emulator support
- Workload Identity on GKE for production auth

## In-Memory Details

- No external dependencies
- `Subscribe` blocks until context cancelled or messages published
- Nack re-publishes the message for re-delivery
- Messages lost on process restart
- Best for development, testing, and single-pod scenarios

## Redis Pub/Sub Details

- At-most-once delivery — messages lost if no subscriber
- JSON wire format for payload + attributes
- All subscribers receive every message (broadcast)
- No ack, no retry, no DLQ — fire and forget
- Best for real-time notifications and cache invalidation
