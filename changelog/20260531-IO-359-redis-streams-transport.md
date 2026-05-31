## IO-359: go-task-orbit — Redis Streams Transport

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | Redis Streams transport backend — durable, consumer-group-based message processing |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Add a Redis Streams transport (`transport/redisstreams/`) implementing `ringq.Transport` with full durability, consumer groups, acknowledgment, pending message recovery, and dead letter support. This replaces the legacy `go-workers` Redis Lists model with a modern stream-based architecture — persistent, replayable, and crash-safe.

#### Why it is needed
Redis Streams is the direct upgrade path from go-workers (Redis Lists). It provides:
- **Persistence** — messages survive crashes (unlike `BRPOP` lists)
- **Consumer groups** — load-balanced delivery across pods (no competing consumers)
- **Acknowledgment** — `XACK` after processing (equivalent to SQS `DeleteMessage`)
- **Pending recovery** — `XPENDING` + `XCLAIM` for visibility timeout (reassign stale messages)
- **Replay** — read from any point in the stream for debugging/audit
- **DLQ** — separate dead letter stream for failed messages

#### Success criteria
- [ ] `transport/redisstreams/` package implementing `ringq.Transport`
- [ ] Publish: `XADD` to stream with topic attribute
- [ ] Subscribe: `XREADGROUP` with consumer group for balanced delivery
- [ ] Ack: `XACK` to acknowledge processed messages
- [ ] Nack: `XCLAIM` for delayed retry (reassign after idle time)
- [ ] SendToDLQ: `XADD` to dead letter stream
- [ ] Stale message recovery: `XPENDING` + `XCLAIM` on startup (pod restart safety)
- [ ] Consumer group auto-creation: `XGROUP CREATE` if not exists
- [ ] Unit tests using miniredis (no external Redis needed)
- [ ] E2E tests with real Redis container

---

### Architecture

```
Producer → XADD orders:stream {payload, X-Topic: email.send}
                    │
                    ▼
            ┌──────────────┐
            │ Redis Stream  │  ← persistent, append-only log
            │  orders:stream│
            └──────┬───────┘
                   │ XREADGROUP (consumer group: orders-workers)
        ┌──────────┼──────────┐
        ▼          ▼          ▼
     Pod 1      Pod 2      Pod 3
   (consumer  (consumer  (consumer
      A)         B)         C)
     Ring       Ring       Ring
     Pool       Pool       Pool
         │          │          │
         └─── XACK ─┴── XCLAIM ─┘
                   │
        ┌──────────┴──────────┐
        │   DLQ Stream        │  ← failed messages
        └─────────────────────┘
```

#### Streams vs go-workers (Redis Lists)

| | go-workers (Lists) | Redis Streams (go-task-orbit) |
|---|---|---|
| Primitive | `BRPOP` from list | `XREADGROUP` from stream |
| Message survival | Gone after `BRPOP` | Persisted until `XACK` |
| Crash safety | Lost in-flight messages | `XPENDING` + `XCLAIM` recovers them |
| Multi-pod | Competing `BRPOP` → one pod gets it | Consumer group balances delivery |
| Visibility timeout | None | `XCLAIM` after idle timeout |
| Dead letter | Custom redis list | Separate DLQ stream |
| Replay | Impossible | Read from any message ID |
| Ordering | FIFO | FIFO per stream |

#### Consumer Group Model

```
Stream: orders:stream
  ├── Consumer Group: orders-workers
  │   ├── Consumer: pod-abc123 (Pod 1)
  │   ├── Consumer: pod-def456 (Pod 2)
  │   └── Consumer: pod-ghi789 (Pod 3)
  └── DLQ Stream: orders:stream:dlq
```

Each pod creates a unique consumer name (e.g., hostname or UUID). The consumer group distributes pending messages across consumers automatically. No central coordinator needed.

---

### Scope Table

| # | Scope | Details | Complexity | Estimate |
|---|---|---|---|---|
| 1 | Redis client + Config struct | `go-redis/redis` v9, connection options, stream key, consumer group, DLQ stream, consumer name | Low | 0.5h |
| 2 | Consumer group bootstrap | `XGROUP CREATE MKSTREAM` on startup. Idempotent — ignores BUSYGROUP error. | Low | 0.5h |
| 3 | Publish implementation | `XADD stream * field value` with topic attribute, payload, idempotency key | Low | 0.5h |
| 4 | Subscribe implementation | `XREADGROUP GROUP group consumer BLOCK timeout STREAMS key >`. Poll loop with batch receive. | Medium | 2h |
| 5 | Ack implementation | `XACK stream group messageID` for each processed message | Low | 0.3h |
| 6 | Nack implementation | `XCLAIM stream group consumer min-idle-time messageID` for delayed retry | Medium | 1h |
| 7 | SendToDLQ implementation | `XADD stream:dlq * payload` with original metadata + error | Low | 0.5h |
| 8 | Stale message recovery | On startup: `XPENDING` for messages claimed by this consumer but unacked → process or claim from others via `XCLAIM` after idle timeout | High | 2h |
| 9 | Graceful shutdown | `XACK` pending messages on shutdown, unsubscribe consumer | Medium | 1h |
| 10 | Unit tests (miniredis) | Test all methods against in-process Redis | Medium | 2h |
| 11 | E2E tests (Redis container) | Full pipeline with real Redis + Docker | Medium | 1.5h |
| 12 | Documentation | README update, transport comparison, go-workers migration guide | Low | 1h |

**Total estimate:** ~12.8h

---

### API Design

```go
package redisstreams

type Config struct {
    Addr            string        // "localhost:6379"
    StreamKey       string        // "orders:stream"
    ConsumerGroup   string        // "orders-workers"
    ConsumerName    string        // auto-generated if empty (hostname)
    DLQStreamKey    string        // "orders:stream:dlq" (optional)
    Password        string
    DB              int
    BlockTimeout    time.Duration // XREADGROUP BLOCK (default: 2s)
    BatchSize       int64         // XREADGROUP COUNT (default: 10)
    IdleTimeout     time.Duration // XCLAIM idle threshold (default: 30s)
    TopicAttribute  string        // message field for topic routing (default: "X-Topic")
}

type StreamsTransport struct {
    client       *redis.Client
    config       Config
    consumerName string
}

func New(cfg Config) *StreamsTransport { ... }
func NewWithClient(client *redis.Client, cfg Config) *StreamsTransport { ... }

// ringq.Transport
func (t *StreamsTransport) Publish(ctx context.Context, msg ringq.Message) error
func (t *StreamsTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error
func (t *StreamsTransport) Ack(ctx context.Context, messages []ringq.Message) error
func (t *StreamsTransport) Nack(ctx context.Context, message ringq.Message, delay time.Duration) error
func (t *StreamsTransport) SendToDLQ(ctx context.Context, message ringq.Message) error
func (t *StreamsTransport) Close() error
```

#### Usage

```go
transport := redisstreams.New(redisstreams.Config{
    Addr:          "localhost:6379",
    StreamKey:     "orders:stream",
    ConsumerGroup: "orders-workers",
    DLQStreamKey:  "orders:stream:dlq",
})

pipeline := ringq.New().
    Transport(transport).
    Handle("email.send", ringq.Wrap(SendEmailHandler)).
    Concurrency(64).
    BufferSize(4096)

pipeline.Run(ctx)
```

---

### Flow Details

#### Subscribe Loop

```
loop:
    XREADGROUP GROUP orders-workers CONSUMER pod-1 BLOCK 2000 COUNT 10 STREAMS orders:stream >
        ↓ messages received
    Convert Redis stream messages → []ringq.Message
        ↓
    Call ringq.ConsumeHandler(batch)
        ↓
    Runtime: ring buffer → dispatch → worker → Ack/Nack/DLQ
```

#### Stale Recovery (on startup)

```
XGROUP CREATE orders:stream orders-workers $ MKSTREAM
    ↓
XPENDING orders:stream orders-workers - + 100
    ↓ for each pending message owned by this consumer
XACK (if processed but never acked — idempotency layer handles re-delivery)
    ↓ for messages owned by OTHER consumers (dead pod)
XCLAIM orders:stream orders-workers pod-1 {min-idle-time} {messageIDs}
    → claim and re-process
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| Redis memory growth (unbounded stream) | Users should configure `MAXLEN` on streams. Library logs warning if stream length exceeds threshold. |
| Consumer group rebalancing | Redis Streams handles this natively. No rebalance protocol needed. |
| go-redis dependency (~20 deps) | Acceptable — teams using Redis transport already have Redis infrastructure. |
| miniredis for testing | miniredis supports Streams since v2.30+. Verify version compatibility. |
| Message ID format | Redis stream message IDs are `timestamp-sequence`. Store in `ringq.Message.ID`. Handle concurrency — multiple messages share same timestamp. |

---

### Assumptions

1. Redis server supports Streams (Redis 5.0+, released 2018)
2. `go-redis/redis` v9 Go client library
3. Consumer group is pre-provisioned OR library creates it on startup
4. Stream keys follow naming convention: `{domain}:{entity}:stream`
5. DLQ stream follows convention: `{stream}:dlq`

---

### Out of Scope

- `MAXLEN` configuration (users apply via Redis directly)
- Stream trimming / TTL management
- Multi-stream consumer groups
- Redis Cluster / Sentinel (single-node or proxied Redis only for v1)
- Redis Pub/Sub (separate plan: `20260531-IO-359-redis-pubsub-transport.md`)

---

### Comparison: All Transports

| | SQS | Pub/Sub | In-Memory | Redis Streams | Redis Pub/Sub |
|---|---|---|---|---|---|
| **Durability** | Persistent | Persistent | None | Persistent | None |
| **Ack** | `DeleteMessageBatch` | `msg.Ack()` | No-op | `XACK` | No-op |
| **Retry** | Visibility timeout | Nack → redelivery | Ring re-insert | `XCLAIM` | No-op |
| **DLQ** | Separate SQS queue | Subscription DLQ policy | No-op | DLQ stream | No-op |
| **Fan-out** | Single consumer | Single subscription | Single pod | Consumer group | All subscribers |
| **Replay** | No | No | No | Yes (any ID) | No |
| **Protocol** | HTTP | gRPC | In-process | TCP (RESP) | TCP (RESP) |
| **Emulator** | Floci | Google emulator | None | miniredis | miniredis |
| **Best for** | AWS prod | GCP prod | Dev/test | Redis shops, migration from go-workers | Real-time broadcast |

---

### Confirmation

- [x] Business goal: Durable Redis Streams transport — direct upgrade from go-workers lists
- [x] Systems impacted: New `transport/redisstreams/` package, e2e tests, README update
- [x] Change approach: Consumer group + XREADGROUP + XACK + XCLAIM for parity with SQS/Pub/Sub transports
- [x] Scope boundaries: Single-node Redis, no Cluster/Sentinel, no MAXLEN management
