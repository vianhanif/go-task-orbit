## IO-359: go-task-orbit — AWS-Native Async Worker Library

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | go-task-orbit: AWS-native async worker library with ring-buffer scheduling |
| Repo | `github.com/vianhanif/go-task-orbit` (new standalone) |
| Go Version | 1.21+ (bumped from 1.19 for GCP Pub/Sub transport compatibility) |
| Branch source | `master` (new repo) |

---

### Task Overview

#### What is changing
Build a new Go library (`go-task-orbit`) that provides an AWS-native async job processing runtime. It replaces the existing `go-workers` library with a ring-buffer-based architecture that separates transport (SQS) from execution scheduling, with pluggable backends for future extensibility (Redis, Kafka, MySQL).

#### Why it is needed
The current `go-workers` library has two primary pain points:
1. **Redis coupling** — mandatory Redis dependency makes local development/testing hard and creates operational bottlenecks
2. **Lack of observability** — no built-in metrics, tracing, or visibility into queue state, throughput, or failures

The library targets `core-api` as its first consumer (I/O-bound + DB-bound workloads: network calls, database operations) running on EKS with KEDA-driven autoscaling.

#### Success criteria
- [ ] SQS transport with batch receive, batch ack, batch visibility extension
- [ ] In-memory ring buffer as local execution scheduler (no Redis dependency)
- [ ] Bounded worker pool (configurable concurrency, no goroutine explosion)
- [ ] Library-managed idempotency (dedupe via SQS MessageAttributes, pluggable store)
- [ ] Explicit handler result model (Ack, Retry, RetryWithDelay, DLQ)
- [ ] Hook-based observability (OnReceive, OnDispatch, OnComplete, OnError, OnRetry, OnDuplicate)
- [ ] Graceful shutdown (SIGTERM → stop polling → drain inflight → ack → exit)
- [ ] Native SQS DLQ support
- [ ] Pipeline/builder chain API with topic-based routing
- [ ] At-least-once delivery with idempotency layer
- [ ] Go 1.19 compatible

---

### Architecture

```
                        +-----------------------+
                        |       Producer        |
                        |  (enqueue to SQS)     |
                        +-----------+-----------+
                                    |
                                    v
                         +----------+----------+
                         |    SQS Main Queue   |
                         +----------+----------+
                                    |
                         batch ReceiveMessage
                                    |
                                    v
                    +---------------+----------------+
                    |         SQS Poller            |
                    |  (long polling, adaptive      |
                    |   prefetch, batch receive)    |
                    +---------------+----------------+
                                    |
                                    v
                    +---------------+----------------+
                    |        Ring Buffer            |
                    |  (in-process scheduler,       |
                    |   bounded, lock-minimized,    |
                    |   cache-friendly)             |
                    +---------------+----------------+
                                    |
                                    v
                    +---------------+----------------+
                    |      Idempotency Filter       |
                    |  (check dedupe store,         |
                    |   ack duplicates silently)    |
                    +---------------+----------------+
                                    |
                                    v
                    +---------------+----------------+
                    |        Worker Pool            |
                    |  (bounded goroutines,         |
                    |   configurable concurrency)   |
                    +---------------+----------------+
                         |              |
                         v              v
                    +--------+    +-----------+
                    |  Ack   |    |  Retry /  |
                    | Batcher|    |   DLQ     |
                    +--------+    +-----------+
                         |              |
                         v              v
                   DeleteMessage   Send to DLQ
                     Batch         or retry queue
```

---

### Multi-Replica Design

The library is designed for horizontal scaling across multiple pod replicas in EKS. Each pod runs its own ring buffer and worker pool independently. The per-pod ring buffer is **intentional and correct** — it provides local scheduling without cross-pod coordination overhead.

```
         ┌─────────┐     ┌─────────┐     ┌─────────┐
         │  Pod A  │     │  Pod B  │     │  Pod C  │
         │ ┌─────┐ │     │ ┌─────┐ │     │ ┌─────┐ │
         │ │Ring │ │     │ │Ring │ │     │ │Ring │ │
         │ │ Buf │ │     │ │ Buf │ │     │ │ Buf │ │
         │ └─────┘ │     │ └─────┘ │     │ └─────┘ │
         │ Workers │     │ Workers │     │ Workers │
         └───┬─────┘     └───┬─────┘     └───┬─────┘
             │               │               │
             └───────────────┼───────────────┘
                             │
                      ┌──────┴──────┐
                      │     SQS     │  ← single source of truth
                      └─────────────┘
```

| Concern | Multi-replica safe? | Why |
|---|---|---|
| Message distribution | Yes | SQS fan-out — each ReceiveMessage returns distinct messages |
| Duplicate delivery | Yes (with idempotency) | SQS visibility timeout prevents in-flight duplicates. Edge case: timeout expiry → caught by idempotency layer |
| Acknowledgment | Yes | DeleteMessage is receipt-handle-specific — two pods can't ack the same handle |
| DLQ routing | Yes | Each pod sends to DLQ independently; SQS handles the move |
| Retries | Yes | Retries re-enter the pod's local ring or SQS visibility timeout handles it |
| KEDA autoscaling | Yes | KEDA reads ApproximateNumberOfMessages from SQS — pod count scales on queue depth |
| Graceful shutdown | Yes | EKS sends SIGTERM per pod; each pod drains its own ring independently |
| Metrics | Yes | Prometheus scrapes per-pod; pod label distinguishes replicas |

**The one gap:** In-memory idempotency does NOT dedupe across pods. See Idempotency Design below.

---

### Scope Table

| # | Scope | Details | Complexity | Recommended LLM | Estimate |
|---|---|---|---|---|---|
| 1 | Module scaffolding | go.mod, directory structure, CI, lint config | Low | Fast | 1h |
| 2 | Core types | Event/Message, Result, Handler[T], Hooks, Codec[T] | Low | Fast | 1h |
| 3 | Transport interface + SQS implementation | Poller, batch receive/ack/visibility, DLQ routing | High | Advanced | 4h |
| 4 | Ring buffer scheduler | Integrate existing ring lib, batch enqueue, batch dispatch, backpressure policies | Medium | Mid | 3h |
| 5 | Worker pool (executor) | Bounded goroutine pool, concurrency config, graceful drain | Medium | Mid | 2h |
| 6 | Idempotency layer | IdemStore interface, MemoryStore + RedisStore implementations, SQS attribute key, TTL, OnDuplicate hook | Medium | Mid | 3h |
| 7 | Retry engine + DLQ coordinator | Explicit Result handling (Ack/Retry/DLQ), immediate retry via ring re-insert. RetryWithDelay re-uses SQS visibility timeout (no timer wheel in v1) | Medium | Mid | 3h |
| 8 | Pipeline builder API | Builder chain (Transport, Handle, Idempotency, Hooks, Concurrency, BufferSize, Run) | Medium | Mid | 3h |
| 9 | Hook system + observability | Lifecycle hooks, OTel wiring examples (not OTel dependency) | Low | Fast | 1h |
| 10 | Graceful shutdown | SIGTERM handling, poller stop, inflight drain, final ack, context cancellation | Medium | Mid | 2h |
| 11 | In-memory transport backend | For local development/testing, replaces Redis dependency | Low | Fast | 2h |
| 12 | Tests | Unit tests for all components, integration test with local SQS (moto/localstack), benchmarks | Medium | Mid | 4h |
| 13 | Documentation | README, godoc, examples, migration guide from go-workers | Low | Fast | 2h |

**Total estimate:** ~31h

---

### API Design (Confirmed)

```go
package ringq

// Pipeline builder — topic-routed
pipeline := ringq.New().
    Transport(sqs.New(sqs.Config{
        QueueURL:      "https://sqs.region.amazonaws.com/account/orders-main",
        DLQURL:        "https://sqs.region.amazonaws.com/account/orders-dlq",
        MaxMessages:   10,
        WaitTime:      20,
    })).
    Handle("email.send", emailHandler).
    Handle("invoice.generate", invoiceHandler).
    Idempotency(ringq.IdempotencyConfig{
        Store:        idempotency.NewMemoryStore(),
        AttributeKey: "IdempotencyKey",
        TTL:          24 * time.Hour,
    }).
    WithHooks(ringq.Hooks{
        OnReceive:   onReceiveHook,
        OnDispatch:  onDispatchHook,
        OnComplete:  onCompleteHook,
        OnError:     onErrorHook,
        OnRetry:     onRetryHook,
        OnDuplicate: onDuplicateHook,
    }).
    Concurrency(64).
    BufferSize(4096).
    Run(ctx)
```

#### Handler + Result

```go
type Handler[T any] func(ctx context.Context, msg T) Result

type Result struct {
    Action Action         // Ack | Retry | RetryWithDelay | DLQ
    Delay  time.Duration  // used with RetryWithDelay
    Err    error
}

const (
    Ack            Action = iota  // success, delete from SQS
    Retry                          // immediate retry (within ring)
    RetryWithDelay                 // retry after Delay (timer wheel)
    DLQ                            // send to dead letter queue
)
```

#### Hooks

```go
type Hooks struct {
    OnReceive   func(ctx context.Context, count int)
    OnDispatch  func(ctx context.Context, topic string)
    OnComplete  func(ctx context.Context, topic string, dur time.Duration)
    OnError     func(ctx context.Context, topic string, err error)
    OnRetry     func(ctx context.Context, topic string, attempt int)
    OnDuplicate func(ctx context.Context, key string)
}
```

---

### Idempotency Design

| Aspect | Decision |
|---|---|---|
| Key source | SQS MessageAttributes (configurable attribute name, default: `"IdempotencyKey"`) |
| Storage backend | Pluggable (`IdemStore` interface), in-memory default (sync.Map + TTL), optional Redis, MySQL |
| TTL | Configurable per pipeline |
| On duplicate | Ack silently (DeleteMessage), fire `OnDuplicate` hook |
| Missing key | Pass through (no dedupe check) — not an error |

```go
type IdemStore interface {
    Exists(ctx context.Context, key string) (bool, error)
    Mark(ctx context.Context, key string, ttl time.Duration) error
    Close() error
}
```

**Store safety by replica count:**

| Store | Single pod | Multi pod (>1) | When to use |
|---|---|---|---|
| `MemoryStore` (default) | Works | Does NOT dedupe across pods | Dev, testing, single-replica deployments |
| `RedisStore` | Works | Works (shared Redis) | Production with multiple replicas |
| `MySQLStore` | Works | Works (shared DB) | Transactional idempotency, audit trail |

> **Production warning:** `MemoryStore` is pod-local. If a duplicate message lands on a different pod (e.g., visibility timeout expiry), the dedupe will miss it and the message will be processed twice. **Always use `RedisStore` (or equivalent shared store) when running >1 replica in production.**

---

### Design Decisions

| Decision | Rationale |
|---|---|
| Ring buffer uses existing Go lib (not written from scratch) | Lower risk, faster delivery, well-tested primitives |
| Fixed ring buffer size (change requires redeploy) | Acceptable tradeoff: size is set per-pod capacity, not per-load. Dynamic scaling is handled by pod count (KEDA). Fixed allocation = predictable latency + no GC pressure. ConfigMap env var + rolling restart makes size changes low-friction. |
| Transport separate from runtime | SQS handles durability; ring handles scheduling. Enables future Redis/Kafka/MySQL backends |
| Hook-based observability, no OTel SDK dependency | Teams already use OTel; hooks give flexibility without forcing dependency |
| Explicit Result type over error-only | Handlers need fine-grained control: retry with delay, skip to DLQ, ack silently |
| Raw `[]byte` transport, default JSON codec | No opinion on serialization; typed handlers get default JSON, raw handlers get bytes |
| At-least-once with idempotency | Adequate for I/O-bound + DB-bound workloads; simpler than exactly-once |
| Go 1.21 target | Required for GCP Pub/Sub transport (`cloud.google.com/go/pubsub`). Upgraded from original 1.19 constraint. |
| Batch APIs everywhere internally | Massive SQS cost reduction, higher throughput |
| Task (taskfile.dev) as build tool | Single YAML-based task runner for test/lint/e2e. Replaces raw shell scripts. Cross-platform, single binary. |

---

### Out of Scope (Phase 2+)

- Redis Streams transport backend
- Kafka transport backend
- MySQL transport backend
- Distributed coordination / leader election
- Exactly-once semantics
- FIFO queue support
- Delayed job scheduling (timer wheel)
- Advanced retry backoff strategies (exponential, jitter)
- Admin UI / dashboard
- Metrics export (Prometheus/CloudWatch endpoints) — hooks enable this, but library won't ship it

---

### Package Structure (Proposed)

```
go-task-orbit/
├── changelog/
│   └── 20260531-IO-359-go-task-orbit-async-worker-library.md
├── README.md
├── go.mod
├── go.sum
│
├── ringq/                   # public API (pipeline builder, core types)
│   ├── ringq.go             # Pipeline builder, New(), Run()
│   ├── types.go             # Handler[T], Result, Hooks, Message
│   ├── codec.go             # Codec[T], JSONCodec
│   ├── hooks.go             # Hooks struct
│   └── config.go            # Config, IdempotencyConfig
│
├── transport/               # Transport interface + implementations
│   ├── transport.go         # Transport interface
│   ├── sqs/                 # SQS transport
│   │   ├── sqs.go           # SQSTransport, config, batch receive/ack
│   │   └── sqs_test.go
│   └── memory/              # In-memory transport (dev/test)
│       ├── memory.go
│       └── memory_test.go
│
├── ring/                    # Ring buffer abstraction
│   ├── buffer.go            # RingBuffer interface, wrapper
│   └── policy.go            # OverflowPolicy (Block, DropNewest, DropOldest, Reject)
│
├── executor/                # Worker pool
│   ├── pool.go              # Bounded goroutine pool
│   └── pool_test.go
│
├── idempotency/             # Idempotency layer
│   ├── idempotency.go       # Filter, IdemStore interface
│   ├── store_memory.go      # In-memory IdemStore (dev/test)
│   ├── store_redis.go       # Redis IdemStore (production multi-pod)
│   └── idempotency_test.go
│
├── retry/                   # Retry engine
│   ├── retry.go             # Retry coordinator, Result handler
│   └── retry_test.go
│
├── lifecycle/               # Graceful shutdown
│   ├── lifecycle.go         # SIGTERM handler, drain logic
│   └── lifecycle_test.go
│
├── internal/                # Internal utilities
│   ├── batch/               # Batch accumulator
│   ├── backoff/             # Backoff strategies
│   └── middleware/          # Middleware chain
│
├── examples/                # Usage examples
│   ├── simple/              # Basic SQS worker
│   ├── idempotent/          # With idempotency
│   └── otel/                # OTel hook wiring
│
└── bench/                   # Benchmarks
    └── bench_test.go
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| Ring buffer library selection | Evaluate 2-3 options (gammazero/deque, etc.) before committing; benchmark with SQS-like workloads |
| Go 1.19 constraint limits stdlib | Acceptable; generics + atomics available; no `slog` means structured logging left to caller |
| SQS batch API complexity | Well-documented AWS SDK v2; batch size/visibility coordination is the main challenge |
| Idempotency store consistency | MemoryStore is pod-local — not safe for multi-replica production. RedisStore required for >1 pod. Library will log a warning if MemoryStore is used with known multi-replica config |
| Graceful drain timing | EKS SIGTERM gives 30s default; library must drain within this window or risk duplicate processing |
| Migration from go-workers | Breaking API change; needs migration guide and possibly a compatibility wrapper |

---

### Assumptions

1. core-api uses AWS SDK Go v2 (`github.com/aws/aws-sdk-go-v2`)
2. SQS queues are pre-provisioned (library does not create/manage queues)
3. Message payloads fit in SQS size limits (256KB)
4. Single-producer, multi-consumer model per queue
5. EKS pods have IAM roles for SQS access (IRSA)
6. The existing ring buffer library chosen will be Go 1.19 compatible

---

### Confirmation

- [x] Business goal: Replace go-workers with AWS-native, observable async worker library
- [x] Systems impacted: New library, consumed by core-api first
- [x] Change approach: Greenfield library, modular packages, SQS-first
- [x] Scope boundaries: Phase 1 = SQS only, no Redis/Kafka/MySQL transports
- [x] API style confirmed: Pipeline/builder chain, topic-routed, generic handlers, explicit Result
- [x] Idempotency confirmed: Library-managed, SQS attribute key, pluggable store, ack-on-duplicate
- [x] Observability confirmed: Hook-based, no OTel SDK dependency
