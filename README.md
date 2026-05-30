# go-task-orbit

Async worker library for Go with ring-buffer scheduling and pluggable transport backends (SQS, Pub/Sub, In-Memory).

**Module:** `github.com/vianhanif/go-task-orbit`

**Go:** 1.21+

**CI:** [![E2E Tests](https://github.com/vianhanif/go-task-orbit/actions/workflows/e2e.yml/badge.svg)](https://github.com/vianhanif/go-task-orbit/actions/workflows/e2e.yml)

<img src="architecture.svg" alt="go-task-orbit architecture" width="800">

## Overview

`go-task-orbit` is an async job processing runtime that combines cloud message brokers with in-process ring-buffer scheduling. It replaces the legacy `go-workers` library with a modern, observable, multi-cloud architecture.

### Architecture

```
Transport (SQS / Pub/Sub / In-Memory) → Ring Buffer → Idempotency Filter → Worker Pool → Ack/Retry/DLQ
```

- **Transport** handles durability, cross-pod delivery, and protocol-specific I/O
- **Ring buffer** handles local scheduling, backpressure, and concurrency control
- **Worker pool** provides bounded goroutine execution (no goroutine explosion)

## Quick Start

### AWS SQS

```go
import (
    "github.com/vianhanif/go-task-orbit/ringq"
    "github.com/vianhanif/go-task-orbit/transport/sqs"
)

pipeline := ringq.New().
    Transport(sqs.New(sqs.Config{
        QueueURL: "https://sqs.us-east-1.amazonaws.com/123456789/orders-main",
        DLQURL:   "https://sqs.us-east-1.amazonaws.com/123456789/orders-dlq",
    })).
    Handle("email.send", ringq.Wrap(SendEmailHandler)).
    Concurrency(64).
    BufferSize(4096)

pipeline.Run(ctx)
```

### GCP Pub/Sub

```go
import (
    "github.com/vianhanif/go-task-orbit/ringq"
    "github.com/vianhanif/go-task-orbit/transport/pubsub"
)

transport, _ := pubsub.New(ctx, pubsub.Config{
    ProjectID:      "my-project",
    TopicID:        "orders",
    SubscriptionID: "orders-sub",
})

pipeline := ringq.New().
    Transport(transport).
    Handle("email.send", ringq.Wrap(SendEmailHandler)).
    Concurrency(64).
    BufferSize(4096)

pipeline.Run(ctx)
```

### Local Development

```go
import "github.com/vianhanif/go-task-orbit/transport/memory"

pipeline := ringq.New().
    Transport(memory.New()).
    Handle("email.send", ringq.Wrap(SendEmailHandler)).
    Concurrency(4).
    BufferSize(1024)

pipeline.Run(ctx)
```

## Transport Backends

| Backend | Status | Protocol | Use case |
|---|---|---|---|
| **Amazon SQS** | Supported | HTTP (REST) | AWS production — batch receive/ack, native DLQ |
| **GCP Pub/Sub** | Supported | gRPC | GCP production — streaming pull, subscription-level DLQ |
| **In-Memory** | Supported | In-process | Development / testing — zero dependencies |
| Redis Streams | Planned | TCP | Alternative production transport |
| Kafka | Planned | TCP | Event-driven architectures |
| MySQL | Planned | TCP | Transactional job queues |

## Features

- **Multi-cloud transport** — SQS (batch I/O), Pub/Sub (gRPC streaming), In-Memory (dev/test)
- **Ring buffer scheduler** — power-of-two sizing, bitwise AND mask, atomic visibility, configurable overflow policies
- **Bounded concurrency** — configurable worker pool, no goroutine leaks
- **Pipeline builder API** — chainable configuration with topic-based handler routing
- **Library-managed idempotency** — dedupe via message attributes, pluggable store (in-memory, Redis)
- **Explicit result model** — Ack, Retry, RetryWithDelay, DLQ
- **Hook-based observability** — wire up OpenTelemetry, logging, or metrics via lifecycle hooks (no OTel SDK dependency)
- **At-least-once delivery** — idempotency layer handles deduplication
- **Graceful shutdown** — SIGTERM-aware draining for EKS/GKE/Kubernetes
- **Type-safe handlers** — Go generics with pluggable codec (JSON default, raw bytes supported)
- **E2E tested** — 13 integration tests across SQS and Pub/Sub against real cloud emulators

## Handler Example

```go
type EmailPayload struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
    Body    string `json:"body"`
}

func SendEmailHandler(ctx context.Context, msg EmailPayload) ringq.Result {
    if err := email.Send(msg.To, msg.Subject, msg.Body); err != nil {
        return ringq.Result{Action: ringq.Retry, Err: err}
    }
    return ringq.Result{Action: ringq.Ack}
}

// Retry with explicit max retries:
pipeline.HandleWithRetry("email.send", ringq.Wrap(SendEmailHandler), 3, 5*time.Second, nil)
```

## Observability

```go
pipeline.WithHooks(ringq.Hooks{
    OnReceive:   func(ctx context.Context, count int) { /* batch receive span */ },
    OnDispatch:  func(ctx context.Context, topic string) { /* dispatch span */ },
    OnComplete:  func(ctx context.Context, topic string, dur time.Duration) { /* latency metric */ },
    OnError:     func(ctx context.Context, topic string, err error) { /* error counter */ },
    OnRetry:     func(ctx context.Context, topic string, msg ringq.Message, attempt int) { /* retry gauge */ },
    OnDuplicate: func(ctx context.Context, key string) { /* duplicate counter */ },
})
```

**All hooks are optional.** The library has zero OpenTelemetry dependency — hooks are plain functions your team wires up.

## Idempotency

The library manages deduplication via a pluggable `IdemStore`. The key is read from message attributes (configurable name, default `IdempotencyKey`).

```go
// Single pod (dev/test):
pipeline.Idempotency(ringq.IdempotencyConfig{
    Store:        idempotency.NewMemoryStore(),
    AttributeKey: "IdempotencyKey",
    TTL:          24 * time.Hour,
})

// Multi-pod production (>1 replica):
pipeline.Idempotency(ringq.IdempotencyConfig{
    Store:        idempotency.NewRedisStore(redisClient, "idem:"),
    AttributeKey: "IdempotencyKey",
    TTL:          24 * time.Hour,
})
```

> **Important:** `MemoryStore` is pod-local. In multi-replica deployments, use `RedisStore`.

## Build Tool

This project uses [Task](https://taskfile.dev/) as the build tool:

```bash
task lint        # go vet
task test        # unit tests
task test-race   # unit tests with race detector
task e2e         # AWS SQS e2e (requires Docker + Floci)
task e2e-gcp     # GCP Pub/Sub e2e (requires Docker + GCloud emulator)
task e2e-all     # all e2e tests
task all         # lint + test + e2e
```

## Status

| Component | Status |
|---|---|
| Core types & interfaces | Done |
| Ring buffer scheduler | Done |
| Worker pool | Done |
| Pipeline builder API | Done |
| Retry engine + DLQ | Done |
| Idempotency layer | Done |
| Graceful shutdown | Done |
| SQS transport | Done |
| GCP Pub/Sub transport | Done |
| In-Memory transport | Done |
| SQS E2E tests (Floci) | Done |
| GCP E2E tests (Emulator) | Done |
| GitHub Actions CI | Done |
| Redis Streams | Planned |
| Kafka | Planned |

## License

MIT
