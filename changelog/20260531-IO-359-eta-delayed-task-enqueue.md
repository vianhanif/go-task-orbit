## IO-359: go-task-orbit — ETA Delayed Task Enqueue

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | ETA delayed task enqueue — timer wheel for all transport options |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Add ETA (Estimated Time of Arrival) delayed task scheduling to the ringq pipeline. A hashed wheel timer runs inside the runtime, holding messages until their `NotBefore` delay expires, then inserting them into the ring buffer for normal processing. This works identically across all transport backends (SQS, Pub/Sub, In-Memory).

Simultaneously, upgrade the default retry strategy from fixed-interval to exponential backoff (max 10 retries).

#### Why it is needed
Current pipeline processes messages immediately upon receipt. Many use cases require delayed execution:
- Scheduled reminders (send email in 1 hour)
- Rate-limited actions (process payment after cooldown)
- Retry with growing intervals (exponential backoff)
- Deferred work (clean up temp files in 24 hours)

Without ETA support, callers must implement their own scheduling layer outside the worker library.

#### Success criteria
- [x] `ringq.Message.NotBefore` field (`time.Duration`) for ETA delay
- [x] Hashed wheel timer (`internal/timerwheel/`) with 1-second tick precision
- [x] Messages with `NotBefore > 0` held in timer wheel until expiry
- [x] Expired messages automatically inserted into ring buffer for processing
- [x] `NotBefore <= 0` returns error on enqueue
- [x] Works across all transports: SQS, Pub/Sub, In-Memory
- [x] For backed transports: visibility timeout / ack deadline extended while waiting
- [x] On pod restart: SQS/Pub/Sub redeliver (visibility expired), timer wheel re-queues
- [x] Graceful shutdown drains timer wheel to ring buffer
- [x] Exponential backoff as default retry strategy (base * 2^attempt, max 10 retries)
- [ ] E2E tests for ETA delay with each transport

---

### Architecture

```
Producer: Publish(msg, NotBefore=1h)
              │
              ▼
┌──────────────────────────┐
│ Transport (SQS/Pub/Sub)  │  ← message stored with X-NotBefore attribute
└──────────┬───────────────┘
           │ poller receives
           ▼
┌──────────────────────────┐
│ NotBefore > 0?           │
│   Yes → Timer Wheel      │  ← hashed wheel, 1s tick
│   No  → Ring Buffer      │
└──────────┬───────────────┘
           │ tick expires
           ▼
┌──────────────────────────┐
│ Ring Buffer              │  ← normal processing from here
└──────────┬───────────────┘
           ▼
       Worker Pool → Ack/Retry/DLQ
```

### Timer Wheel Design

**Hashed wheel timer** — O(1) insert, O(1) per-message expiry, unbounded capacity.

```
Timer Wheel (hashed, not fixed-slot)

   tick at 1s intervals
        │
        ▼
   ┌─────────────────────────────────┐
   │  Buckets (map[tick] → []Message) │
   │                                  │
   │  tick 100: [msg A]               │  ← expires in ~100s
   │  tick 3600: [msg B, msg C]       │  ← expires in ~1hr
   │  tick 86400: [msg D]             │  ← expires in ~24hr
   └─────────────────────────────────┘
        │
   current tick advances
        │
   expired bucket → drain to ring buffer
```

Each tick represents 1 second. Bucket key = `(now + NotBefore) / tickInterval`. When the current tick matches a bucket key, all messages in that bucket are moved to the ring buffer.

**Why hashed wheel over fixed-slot:**
- Fixed-slot wheel has `N` slots and wraps around — max delay is `N * tickInterval`
- Hashed wheel uses a map — any duration, no wrap-around, unbounded
- Memory: O(pending messages), not O(slots)

### Durability Model

| Transport | Before ETA | Pod restart | After ETA |
|---|---|---|---|
| **SQS** | Message in SQS queue + timer wheel. Visibility timeout extended until ETA expires. | SQS redelivers (visibility expired). Poller checks NotBefore again, re-enters timer wheel. | Enters ring buffer → processed → acked. |
| **Pub/Sub** | Message in subscription + timer wheel. Ack deadline extended until ETA expires. | Pub/Sub redelivers (ack deadline expired). Same re-queue logic. | Enters ring buffer → processed → acked. |
| **In-Memory** | Message in timer wheel only. No transport layer. | Lost on restart. Consistent with ring buffer behavior. | Enters ring buffer → processed. |

**Visibility timeout management:**
- SQS: `ChangeMessageVisibility` to `NotBefore` duration + buffer
- Pub/Sub: `ModifyAckDeadline` to `NotBefore` duration + buffer
- Extended again if timer tick is approaching expiry without processing

---

### Scope Table

| # | Scope | Details | Complexity | Recommended LLM | Estimate |
|---|---|---|---|---|---|
| 1 | `ringq.Message.NotBefore` field | Add `NotBefore time.Duration` to Message. Default 0 = immediate. | Low | Fast | 0.3h |
| 2 | Hashed wheel timer | `internal/timerwheel/` — O(1) insert, O(1) expire, 1s tick, unbounded capacity | High | Advanced | 3h |
| 3 | Timer integration in runtime | New `runTimer` goroutine. Poller routes `NotBefore > 0` messages to timer. Timer drains expired to ring. | Medium | Mid | 2h |
| 4 | Visibility timeout extension | SQS: `ChangeMessageVisibility` to `NotBefore + 60s`. Pub/Sub: `ModifyAckDeadline` similarly. Loop extends while waiting. | Medium | Mid | 2h |
| 5 | Graceful shutdown drain | On SIGTERM: drain timer wheel to ring buffer before closing. Ensures no messages lost on intentional shutdown. | Medium | Mid | 1h |
| 6 | Exponential backoff retry | Change default retry: base=1s, factor=2, max=10. `delay = min(base * 2^(attempt-1), 5min)`. Global default. | Medium | Mid | 1.5h |
| 7 | Publish with NotBefore attribute | SQS/Pub/Sub Publish includes `X-NotBefore` attribute so poller can reconstruct delay on restart | Low | Fast | 0.5h |
| 8 | E2E test: SQS delayed task | Publish with NotBefore=3s. Verify handler called after delay, not before. | Medium | Mid | 1h |
| 9 | E2E test: Pub/Sub delayed task | Same as SQS but with Pub/Sub transport. | Medium | Mid | 1h |
| 10 | E2E test: restart resilience | Publish delayed message, kill pipeline before ETA, restart, verify delivery. | High | Mid | 2h |
| 11 | E2E test: exponential backoff | Handler fails repeatedly. Verify delay grows (1s, 2s, 4s, 8s...) up to max 10 retries. | Medium | Mid | 1h |
| 12 | Unit tests: timer wheel | Insert/expire/drain/empty/ordering correctness | Medium | Mid | 1.5h |
| 13 | Documentation | README, godoc, examples/delayed/ example app | Low | Fast | 1h |

**Total estimate:** ~17.8h

---

### API Design

#### Message field

```go
type Message struct {
    ID            string
    Topic         string
    Payload       []byte
    Attributes    map[string]string
    ReceiptHandle string
    Attempts      int
    NotBefore     time.Duration  // NEW: minimum delay before processing (0 = immediate)
}
```

#### Publish with delay

```go
// SQS
transport.Publish(ctx, ringq.Message{
    ID:        "1",
    Topic:     "email.send",
    Payload:   []byte(`{"to":"user@example.com"}`),
    NotBefore: 1 * time.Hour,   // process in 1 hour
})

// Pub/Sub — identical API
pubsubTransport.Publish(ctx, ringq.Message{
    ID:        "1",
    Topic:     "email.send",
    Payload:   []byte(`{"to":"user@example.com"}`),
    NotBefore: 30 * time.Minute,
})

// Error case
msg := ringq.Message{NotBefore: -1 * time.Second}
err := transport.Publish(ctx, msg)
// err: "ringq: NotBefore must be positive or zero"
```

#### Exponential backoff (new default)

```go
// Before (current):
maxRetries: 3, baseDelay: 5s  → retries at: 5s, 5s, 5s

// After (new default):
maxRetries: 10, baseDelay: 1s  → retries at: 1s, 2s, 4s, 8s, 16s, 32s, 64s, 128s, 256s, 300s (capped at 5min)
```

Formula: `delay = min(baseDelay * 2^(attempt-1), maxDelay)` where `maxDelay = 5 * time.Minute`.

---

### Package Structure (new/modified)

```
go-task-orbit/
├── internal/
│   └── timerwheel/           # NEW
│       ├── wheel.go           # Hashed wheel: Insert, Tick, Drain, Len, Close
│       └── wheel_test.go
├── ringq/
│   ├── ringq.go              # MODIFIED: runtime gains timer wheel, runTimer goroutine
│   ├── types.go              # MODIFIED: Message.NotBefore field
│   ├── handler.go            # MODIFIED: Wrap checks NotBefore
│   └── filter.go             # MODIFIED: filter passes NotBefore through
├── transport/
│   ├── sqs/sqs.go            # MODIFIED: Publish includes X-NotBefore attribute. Poller checks it.
│   └── pubsub/pubsub.go      # MODIFIED: Same as SQS
├── e2e/
│   ├── eta_test.go           # NEW: delayed task tests (e2e tag)
│   └── eta_pubsub_test.go    # NEW: Pub/Sub delayed tests (e2e_gcp tag)
└── examples/
    └── delayed/              # NEW: example with ETA delay
        └── main.go
```

---

### Timer Wheel Interface

```go
// internal/timerwheel/wheel.go
package timerwheel

type Wheel struct {
    mu      sync.Mutex
    buckets map[int64][]interface{}  // tick → messages
    current int64                     // current tick
    tickC   chan struct{}             // 1 tick = 1 second
    closed  chan struct{}
}

func New() *Wheel

// Insert a message. delay is the duration from now when it should expire.
func (w *Wheel) Insert(item interface{}, delay time.Duration)

// Start ticking. Returns a channel that receives expired items.
func (w *Wheel) Start(ctx context.Context) <-chan []interface{}

// Len returns pending message count.
func (w *Wheel) Len() int

// Drain returns all pending messages (for shutdown).
func (w *Wheel) Drain() []interface{}

// Close stops the wheel.
func (w *Wheel) Close()
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| Timer wheel memory growth (unbounded pending) | Log warning if pending > 100K. Users should set up alerts on pending count via hooks. |
| SQS visibility timeout max 12 hours | For ETA > 12h: extend visibility in 11h increments via loop. |
| Pub/Sub ack deadline max 10 minutes | Extend in 9-minute increments via loop. Standard Pub/Sub pattern. |
| Clock skew between pods | Each pod's timer wheel uses local clock. SQS visibility expiry handles redelivery — safe. |
| Exponential backoff may be too aggressive | Cap max delay at 5 minutes. Users can override via `HandleWithRetry`. |
| ETA + idempotency interaction | Idempotency filter runs BEFORE timer wheel. Duplicate detection works as normal — the first message claims the key. |

---

### Assumptions

1. SQS `ChangeMessageVisibility` API is called periodically to extend timeout while in timer wheel
2. Pub/Sub `ModifyAckDeadline` API similarly extends deadline
3. Hashed wheel with `map[int64][]interface{}` is sufficient for up to 500K pending messages
4. `NotBefore` is serialized/deserialized as part of message attributes during transport
5. Exponential backoff formula: `min(1s * 2^(attempt-1), 5min)` with max 10 attempts
6. Timer wheel goroutine lifecycle: start in `Run()`, stop in graceful shutdown drain

---

### Out of Scope

- Cron-like recurring schedules (separate feature)
- Priority queue within the timer wheel (FIFO within same tick is fine)
- Message persistence of In-Memory timer wheel (lost on restart is acceptable)
- SQS `DelaySeconds` integration (pipeline-level timer handles all delays)
- Exactly-once delivery for delayed messages (at-least-once + idempotency is sufficient)

---

### Confirmation

- [x] Business goal: Enqueue tasks with ETA delay, works across all transports
- [x] Systems impacted: `ringq`, `internal/timerwheel`, `transport/sqs`, `transport/pubsub`, e2e tests
- [x] Change approach: Pipeline-level hashed wheel timer + transport visibility extension
- [x] Scope boundaries: Single ETA per message, no recurring schedules, no priority queue
- [x] API: `Message.NotBefore time.Duration` field, error if <= 0
- [x] Timer: 1s tick, hashed wheel, unbounded capacity
- [x] Retry: Exponential backoff as global default (1s * 2^attempt, max 10, cap 5min)
- [x] Durability: SQS/Pub/Sub survive restart via visibility timeout; In-Memory lost
