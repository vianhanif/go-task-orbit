## IO-359: go-task-orbit — GCP Pub/Sub Transport Backend

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | GCP Pub/Sub transport backend for go-task-orbit |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ (bumped from 1.19 for `cloud.google.com/go/pubsub` compatibility) |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Add a new transport backend (`transport/pubsub/`) that implements `ringq.Transport` using Google Cloud Pub/Sub. This enables go-task-orbit to run in GCP environments (GKE, Cloud Run, Compute Engine) with Pub/Sub as the durable message layer — same architecture as the SQS backend, different cloud provider.

#### Why it is needed
go-task-orbit currently supports only AWS SQS and in-memory transports. Teams deploying to GCP need a native transport that:
- Uses Pub/Sub for durability and cross-pod delivery
- Integrates with GCP IAM (Workload Identity on GKE)
- Supports dead letter topics (Pub/Sub native DLQ)
- Has the same programming model as the SQS transport

#### Success criteria
- [ ] `transport/pubsub` package implementing `ringq.Transport` interface
- [ ] Publish: send messages to a Pub/Sub topic
- [ ] Subscribe: pull from a Pub/Sub subscription, feed messages individually to ring buffer
- [ ] Ack: acknowledge processed messages
- [ ] Nack: nack messages with delay (modify ack deadline, same as SQS visibility timeout)
- [ ] SendToDLQ: Nack the message — relies on subscription-level dead letter topic policy (no separate DLQ topic in config)
- [ ] Pub/Sub message attributes → `ringq.Message.Attributes` (for topic routing + idempotency key)
- [ ] Topic-based routing: Pub/Sub attributes carry the topic name via `"X-Topic"` attribute
- [ ] Unit tests using floci-gcp emulator (optional — skip if not available)
- [ ] Full pipeline e2e tests using floci-gcp + GitHub Actions (same scenarios as SQS e2e)

---

### Architecture

```
                         GCP Environment
┌─────────────────────────────────────────────────┐
│                                                 │
│  Producer ──→ Pub/Sub Topic                     │
│                  │                              │
│                  ▼                              │
│          Pub/Sub Subscription                   │
│          (with dead letter topic policy)        │
│                  │                              │
│     ┌────────────┼────────────┐                 │
│     ▼            ▼            ▼                 │
│  ┌──────┐   ┌──────┐    ┌──────┐               │
│  │ Pod 1│   │ Pod 2│    │ Pod 3│               │
│  │ Ring │   │ Ring │    │ Ring │               │
│  │ Pool │   │ Pool │    │ Pool │               │
│  └──────┘   └──────┘    └──────┘               │
│     │            │            │                 │
│     └──── Ack ───┴── Nack ───┘                 │
│              │                                  │
│     Nack triggers redelivery                   │
│     After max attempts → DLQ topic (automatic) │
└─────────────────────────────────────────────────┘
```

#### Pub/Sub ↔ go-task-orbit Mapping

| Pub/Sub Concept | go-task-orbit Mapping |
|---|---|
| Topic | Source of messages (the "queue") |
| Subscription | Per-pipeline pull subscription |
| Message | `ringq.Message` (data = payload, attributes = metadata + topic + idempotency key) |
| Ack | `ringq.Transport.Ack()` → `msg.Ack()` |
| Nack | `ringq.Transport.Nack()` → `msg.Nack()` (or modify ack deadline for delay) |
| Dead Letter Topic | `ringq.Transport.SendToDLQ()` → `msg.Nack()` — relies on subscription DLQ policy |
| Message attributes | `ringq.Message.Attributes` ↔ Pub/Sub attributes map |
| Ordering key | Optional — not used in v1 |

#### Topic Strategy

Single-topic design (recommended for Phase 1):
```
Topic: go-task-orbit-orders    ← all messages published here
Subscription: go-task-orbit-orders-sub  ← pull subscription
```
The subscription is expected to be pre-configured with a dead letter topic policy and max delivery attempts. The library does NOT manage topic/subscription/DLQ provisioning.

```go
attributes := map[string]string{
    "X-Topic":         "email.send",
    "IdempotencyKey":  "abc-123",
}
```

Phase 2 could add multi-topic support (one Pub/Sub topic per go-task-orbit topic).

---

### Scope Table

| # | Scope | Details | Complexity | Recommended LLM | Estimate |
|---|---|---|---|---|---|
| 1 | Pub/Sub Config struct | ProjectID, TopicID, SubscriptionID, MaxMessages, TopicAttribute | Low | Fast | 0.5h |
| 2 | Client initialization | `pubsub.NewClient()`, emulator detection (`PUBSUB_EMULATOR_HOST`), credential loading | Low | Fast | 0.5h |
| 3 | Publish implementation | `topic.Publish()` with attributes, block until server confirms | Low | Fast | 1h |
| 4 | Subscribe implementation | Pull subscription with `subscription.Receive()`, batch handler, context cancellation | Medium | Mid | 2h |
| 5 | Ack implementation | `msg.Ack()` — no batch API in Pub/Sub (ack individually) | Low | Fast | 0.5h |
| 6 | Nack implementation | `msg.Nack()` — tells Pub/Sub to redeliver. For delay: use `ModifyAckDeadline` | Medium | Mid | 1h |
| 7 | SendToDLQ implementation | `msg.Nack()` — relies on subscription-level dead letter topic policy. No separate DLQ topic needed. | Low | Fast | 0.3h |
| 8 | Message conversion | Pub/Sub `*pubsub.Message` ↔ `ringq.Message` (data, attributes, message ID) | Low | Fast | 0.5h |
| 9 | Unit tests | Test Publish/Subscribe/Ack/Nack using floci-gcp emulator | Medium | Mid | 2h |
| 10 | E2E tests | Full pipeline e2e with floci-gcp — same 8 scenarios as SQS e2e (happy path, retry, DLQ, idempotency, batch, etc.) | Medium | Mid | 2h |
| 11 | Taskfile.yml update | Add GCP e2e tasks (floci-gcp-start/stop, e2e-gcp, e2e-gcp-ci) to existing Taskfile | Low | Fast | 0.5h |
| 12 | Documentation | README update (transport table), godoc, example code, SQS vs Pub/Sub comparison | Low | Fast | 1h |

**Total estimate:** ~12.3h

---

### API Design

```go
package pubsub

import (
    "github.com/vianhanif/go-task-orbit/ringq"
)

type Config struct {
    ProjectID      string
    TopicID        string
    SubscriptionID string
    MaxMessages    int    // default: 10 (controls Receive concurrency)
    TopicAttribute string // default: "X-Topic"
}

type PubSubTransport struct {
    client *pubsub.Client
    config Config
    topic  *pubsub.Topic
    sub    *pubsub.Subscription
}

func New(ctx context.Context, cfg Config) (*PubSubTransport, error) { ... }
func NewWithClient(client *pubsub.Client, cfg Config) (*PubSubTransport, error) { ... }

// Implements ringq.Transport
func (t *PubSubTransport) Publish(ctx context.Context, msg ringq.Message) error { ... }
func (t *PubSubTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error { ... }
func (t *PubSubTransport) Ack(ctx context.Context, messages []ringq.Message) error { ... }
func (t *PubSubTransport) Nack(ctx context.Context, message ringq.Message, delay time.Duration) error { ... }
func (t *PubSubTransport) SendToDLQ(ctx context.Context, message ringq.Message) error { ... }
func (t *PubSubTransport) Close() error { ... }
```

#### Subscribe Flow

Each received message invokes the `ringq.ConsumeHandler` callback with a 1-element batch. The ring buffer accumulates these naturally:

```go
func (t *PubSubTransport) Subscribe(ctx context.Context, handler ringq.ConsumeHandler) error {
    return t.sub.Receive(ctx, func(gcpCtx context.Context, msg *pubsub.Message) {
        ringqMsg := t.toRingqMessage(msg)
        // Feed individually — ring buffer accumulates into batches for the dispatcher
        if err := handler(gcpCtx, []ringq.Message{ringqMsg}); err != nil {
            msg.Nack()
            return
        }
        // Note: Transport does NOT auto-ack. The runtime calls t.Ack() after successful processing.
    })
}
```

#### DLQ Strategy

`SendToDLQ()` simply calls `msg.Nack()`. The Pub/Sub subscription must be pre-configured with:
- Dead letter topic (set at subscription creation time)
- Max delivery attempts (e.g., 5)

After max delivery attempts, Pub/Sub automatically moves the message to the DLQ topic. No separate `DLQTopicID` config — this is subscription-level infrastructure.

#### Emulator Detection

```go
func New(ctx context.Context, cfg Config) (*PubSubTransport, error) {
    // floci-gcp sets PUBSUB_EMULATOR_HOST=localhost:4588
    // Google's pubsub emulator sets PUBSUB_EMULATOR_HOST=localhost:8085
    if os.Getenv("PUBSUB_EMULATOR_HOST") != "" {
        // Emulator mode — no credentials needed
        client, err := pubsub.NewClient(ctx, cfg.ProjectID)
        ...
    }
    // Production mode — uses ADC (Application Default Credentials)
    client, err := pubsub.NewClient(ctx, cfg.ProjectID)
    ...
}
```

---

### Package Structure (new)

```
go-task-orbit/
├── Taskfile.yml                  # Shared task runner (includes GCP e2e tasks)
├── transport/
│   ├── transport.go
│   ├── sqs/
│   │   └── ...
│   ├── memory/
│   │   └── ...
│   └── pubsub/                  # NEW
│       ├── pubsub.go            # PubSubTransport, Config, New()
│       ├── convert.go           # toRingqMessage, fromRingqMessage
│       └── pubsub_test.go       # Unit tests with floci-gcp
├── e2e/
│   ├── pubsub_test.go           # Full pipeline e2e with floci-gcp
│   └── ... (SQS e2e)
├── .github/
│   └── workflows/
│       └── e2e.yml              # Single workflow: SQS + GCP e2e (both use Task)
```

#### Taskfile.yml (GCP additions)

```yaml
# Added to existing Taskfile.yml
vars:
  FLOCI_GCP_IMAGE: floci/floci-gcp:latest
  FLOCI_GCP_PORT: '4588'

tasks:
  # ... existing SQS tasks ...

  e2e-gcp:
    desc: Run GCP e2e tests with floci-gcp (local Docker)
    cmds:
      - task: floci-gcp-start
      - defer: { task: floci-gcp-stop }
      - go test -tags=e2e_gcp -v -count=1 -timeout=120s ./transport/pubsub/... ./e2e/...

  e2e-gcp-ci:
    desc: Run GCP e2e tests in CI
    cmds:
      - go test -tags=e2e_gcp -v -count=1 -timeout=120s ./transport/pubsub/... ./e2e/...

  floci-gcp-start:
    desc: Start Floci GCP emulator
    cmds:
      - docker run -d --name floci-gcp-e2e -p {{.FLOCI_GCP_PORT}}:{{.FLOCI_GCP_PORT}} {{.FLOCI_GCP_IMAGE}}
      - until curl -s http://localhost:{{.FLOCI_GCP_PORT}}/_floci/health; do sleep 0.5; done

  floci-gcp-stop:
    desc: Stop and remove Floci GCP container
    cmds:
      - docker rm -f floci-gcp-e2e 2>/dev/null || true
    ignore_error: true

  e2e-all:
    desc: Run all e2e tests (SQS + GCP)
    cmds:
      - task: e2e
      - task: e2e-gcp
```

---

### GitHub Actions (Combined E2E)

Single workflow for both SQS and GCP e2e — both use Task:

```yaml
# .github/workflows/e2e.yml
name: E2E Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  e2e-aws:
    runs-on: ubuntu-latest
    services:
      floci:
        image: floci/floci:latest
        ports:
          - 4566:4566
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      - uses: arduino/setup-task@v2
        with:
          version: '3.x'
      - run: task e2e-ci

  e2e-gcp:
    runs-on: ubuntu-latest
    services:
      floci-gcp:
        image: floci/floci-gcp:latest
        ports:
          - 4588:4588
    env:
      PUBSUB_EMULATOR_HOST: localhost:4588
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version: '1.21'
      - uses: arduino/setup-task@v2
        with:
          version: '3.x'
      - run: task e2e-gcp-ci
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| Go version bump to 1.21+ | Go 1.21 is the minimum for `cloud.google.com/go/pubsub`. Core-api must upgrade. Entire project bumps to 1.21 — single go.mod. |
| Pub/Sub library `cloud.google.com/go/pubsub` is large (~50+ deps) | Acceptable — module lazy loading means SQS-only users don't download pubsub deps |
| Emulator vs real Pub/Sub behavior differences | Test against floci-gcp (primary) + document known gaps for real GCP production |
| Pub/Sub has no batch Ack API (Ack one-at-a-time) | Acceptable — Pub/Sub Acks are fast (gRPC streaming). SQS batch Ack is for cost reduction. |
| DLQ requires subscription-level configuration | Document that users must create subscriptions with DLQ policy. Library does not manage topics/subs/DLQ. |
| Pub/Sub Receive concurrency (per-message callbacks) | `Receive` opens multiple gRPC streams (controlled by `ReceiveSettings.NumGoroutines`). Default is 10. Adequate for Phase 1. |

---

### Assumptions

1. GCP Pub/Sub topics and subscriptions are pre-provisioned (library does not create/manage them)
2. Pub/Sub subscription is configured with dead letter topic at creation time
3. `ringq.Message.Topic` is stored as a Pub/Sub attribute (`"X-Topic"`)
4. `ringq.Message.Attributes` map 1:1 to Pub/Sub message attributes
5. GKE pods use Workload Identity for Pub/Sub access (no service account keys)
6. Pub/Sub message ordering is not required for v1 (standard subscription)
7. `cloud.google.com/go/pubsub` Go module is compatible with Go 1.21+ (project go.mod bumped accordingly)

---

### Out of Scope

- Pub/Sub topic/subscription auto-provisioning
- Pub/Sub ordering keys (FIFO)
- Pub/Sub exactly-once delivery
- Multi-topic architecture (Phase 2)
- GCP Cloud Tasks transport
- Integration with real GCP Pub/Sub (requires cloud credentials)
- Azure Service Bus transport (separate task)

---

### Comparison: SQS vs Pub/Sub Transport

| Feature | SQS Transport | Pub/Sub Transport |
|---|---|---|
| Protocol | HTTP (REST) | gRPC |
| Receive model | Long poll, batch (up to 10) | Streaming pull, per-message callback |
| Ack model | DeleteMessageBatch | `msg.Ack()` (individual) |
| Nack model | ChangeMessageVisibility | `msg.Nack()` or ModifyAckDeadline |
| DLQ | Separate SQS queue | Subscription-level dead letter topic (automatic after max deliveries) |
| Message attributes | SQS MessageAttributes (String type) | Pub/Sub attributes (key-value strings) |
| Max message size | 256 KB | 10 MB |
| Emulator | floci (port 4566) | floci-gcp (port 4588) |

---

### Confirmation

- [x] Business goal: Enable go-task-orbit to run on GCP with Pub/Sub as transport
- [x] Systems impacted: New `transport/pubsub/` package, new e2e tests, new CI workflow
- [x] Change approach: Implement `ringq.Transport` interface using `cloud.google.com/go/pubsub`
- [x] Scope boundaries: Standard Pub/Sub (no ordering keys, no exactly-once). Topics/subs are pre-provisioned.
