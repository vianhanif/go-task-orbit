## IO-359: go-task-orbit — Redis Pub/Sub Transport

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | Redis Pub/Sub transport backend |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Add a lightweight Redis Pub/Sub transport (`transport/redis/`) implementing `ringq.Transport` for fire-and-forget broadcast messaging. Messages are published to Redis channels and delivered to all subscribers. No durability, no acknowledgment, no retry — suitable for real-time notifications, live dashboards, and non-critical event broadcasting.

#### Why it is needed
Teams already running Redis for caching or go-workers can incrementally adopt go-task-orbit without migrating queues to SQS/Pub/Sub. Redis Pub/Sub provides a simple entry point for low-risk workloads (notifications, live updates) before upgrading to Redis Streams for durable processing.

#### Success criteria
- [ ] `transport/redis/` package implementing `ringq.Transport` interface
- [ ] Publish: `PUBLISH` to Redis channel
- [ ] Subscribe: `SUBSCRIBE` / `PSUBSCRIBE` for channel patterns
- [ ] Batch receive: Redis pub/sub delivers each message in a callback; ring buffer accumulates
- [ ] Ack: no-op (Pub/Sub has no acknowledgment)
- [ ] Nack: no-op (can't put a message back)
- [ ] SendToDLQ: no-op (fire-and-forget — consider using Redis Streams for DLQ needs)
- [ ] Channel-pattern routing: `X-Topic` attribute maps to Redis channel subscription
- [ ] Unit tests using in-process Redis (miniredis or real Redis)

---

### Architecture

```
Producer → PUBLISH orders:created {payload}
                │
                ▼
         ┌─────────────┐
         │    Redis     │  ← channel-based broadcast
         └──────┬──────┘
                │ SUBSCRIBE orders:*
    ┌───────────┼───────────┐
    ▼           ▼           ▼
  Pod 1      Pod 2      Pod 3
  Ring       Ring       Ring
  Pool       Pool       Pool
```

#### Pub/Sub Characteristics

| Property | Behavior |
|---|---|
| Delivery | At-most-once — message lost if no subscriber |
| Fan-out | All subscribers receive every message |
| Ordering | Per-channel FIFO, no cross-channel ordering |
| Retry | None — no acknowledgment protocol |
| DLQ | Not applicable — no concept of dead lettering |
| Backpressure | Subscriber must consume faster than publish rate, or Redis buffers in memory |

#### When to use

| Use case | Suitable? |
|---|---|
| Real-time notifications (push, SSE, WebSocket) | Yes |
| Live dashboard updates | Yes |
| Cache invalidation broadcasts | Yes |
| Job processing with retry/DLQ | **No** — use Redis Streams instead |
| Durable message queue | **No** — messages lost on disconnect |

---

### Scope Table

| # | Scope | Details | Complexity | Estimate |
|---|---|---|---|---|
| 1 | Redis client setup | `go-redis/redis` client, connection options, channel config | Low | 0.5h |
| 2 | Publish implementation | `PUBLISH channel msg` with topic attribute serialization | Low | 0.5h |
| 3 | Subscribe implementation | `SUBSCRIBE` with goroutine-per-channel, message → ringq.Message conversion | Medium | 1.5h |
| 4 | Ack / Nack / DLQ stubs | No-op implementations with clear documentation of limitations | Low | 0.3h |
| 5 | Graceful unsubscribe | Handle `Close()`: unsubscribe channels, close connection | Low | 0.5h |
| 6 | Unit tests | Using miniredis (no external Redis needed) | Medium | 1.5h |
| 7 | Documentation | README update, transport comparison, caveat section | Low | 0.5h |

**Total estimate:** ~5.3h

---

### API Design

```go
package redis

type Config struct {
    Addr            string   // "localhost:6379"
    Channels        []string // ["orders:created", "users:*"]
    Password        string
    DB              int
    TopicAttribute  string   // default: "X-Topic"
}

type RedisTransport struct {
    client *redis.Client
    config Config
    pubsub *redis.PubSub
}

func New(cfg Config) *RedisTransport { ... }

// ringq.Transport implementation
func (t *RedisTransport) Publish(ctx context.Context, msg ringq.Message) error
func (t *RedisTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error
func (t *RedisTransport) Ack(ctx context.Context, messages []ringq.Message) error   // no-op
func (t *RedisTransport) Nack(ctx context.Context, msg ringq.Message, delay time.Duration) error // no-op
func (t *RedisTransport) SendToDLQ(ctx context.Context, msg ringq.Message) error    // no-op
func (t *RedisTransport) Close() error
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| At-most-once delivery | Documented as intentional limitation. Users needing durability use Redis Streams. |
| Redis connection loss | Reconnect loop with exponential backoff. Messages lost during disconnect. |
| Subscriber bottleneck | Single Redis connection per transport. Use horizontal scaling for throughput. |
| No DLQ support | Not applicable to Pub/Sub model. Handle failures in handler logic. |

---

### Assumptions

1. Redis server is accessible (local or managed)
2. `go-redis/redis` v9 Go client library
3. Channels are pre-configured — library does not create/manage channels
4. For production use cases requiring durability, users upgrade to Redis Streams transport

---

### Out of Scope

- Message persistence (use Redis Streams)
- Message acknowledgment (can't — pub/sub protocol limitation)
- Dead letter queue (can't — no message state)
- Consumer groups (use Redis Streams)
- Channel auto-provisioning

---

### Confirmation

- [x] Business goal: Lightweight Redis broadcast transport for real-time messaging
- [x] Systems impacted: New `transport/redis/` package, README update
- [x] Scope boundaries: Fire-and-forget only. No durability, no ack, no DLQ.
- [x] Positioning: Entry-level Redis transport — upgrade to Redis Streams for durability
