## IO-359: go-task-orbit — Pub/Sub Transport Known Issues Fix

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | Fix Pub/Sub transport Nack redelivery loop and DLQ behavior |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Fix three related issues in the Pub/Sub transport caused by `Nack()` causing unbounded redelivery loops:

1. **Nack redelivery loop**: DLQ and UnknownTopic tests fire `OnError` 50+ times because `SendToDLQ()` calls `msg.Nack()`, which causes Pub/Sub to redeliver the message immediately. Without a subscription-level DLQ policy, redelivery continues indefinitely.
2. **Attempts counter reset**: `ringq.Message.Attempts` is in-memory only. Each Pub/Sub redelivery creates a new `Message` instance with `Attempts=0`, making retry limits ineffective.
3. **Backoff test skipped**: `TestE2EGCPETABackoff` is permanently skipped because the retry counter never reaches the max limit.

#### Why it is needed
The Pub/Sub transport is advertised as "Supported" but its DLQ/retry behavior is broken in production — messages loop indefinitely when Nack'd without a DLQ topic configured on the subscription. This needs to work correctly before the transport can be considered production-ready.

---

### Root Cause Analysis

```
Handler returns DLQ
    → runtime calls transport.SendToDLQ()
      → Pub/Sub: msg.Nack()
        → Pub/Sub redelivers immediately (no DLQ topic on subscription)
          → Poller receives again
            → Handler returns DLQ again
              → ... infinite loop
```

The fix requires two changes:
1. **Persist Attempts in Pub/Sub attributes** so retry count survives redelivery
2. **After max attempts, stop processing** by either:
   - Publishing to a separate DLQ topic (transport-managed, like SQS)
   - Or Ack'ing the message silently after max attempts (prevents loop)

---

### Scope Table

| # | Scope | Details | Complexity | Estimate |
|---|---|---|---|---|
| 1 | Persist Attempts | Add `X-Attempts` attribute to Publish. Parse in toRingqMessage. Increment on each retry. | Low | 0.5h |
| 2 | Max retries check in Subscribe | Before calling handler, check if Attempts >= maxRetries. If so, Ack silently (or route to DLQ topic). | Medium | 1h |
| 3 | DLQ topic support in Config | Add `DLQTopicID` back to Config for explicit DLQ routing (like SQS DLQ URL). Publish to DLQ topic on max retries. | Medium | 1h |
| 4 | Default max retries | Default max retries = 10 (matching ringq default). Configurable per transport. | Low | 0.3h |
| 5 | Un-skip backoff test | Re-enable `TestE2EGCPETABackoff` with persisted Attempts. | Medium | 0.5h |
| 6 | Fix DLQ test Nack loop | DLQ test should fire OnError once (not 50 times). | Medium | 0.5h |
| 7 | Fix UnknownTopic test Nack loop | UnknownTopic test should fire OnError once (not 50 times). | Medium | 0.5h |

**Total estimate:** ~4.3h

---

### Design

#### Approach A: DLQ Topic (Recommended)

Same model as SQS — a separate DLQ topic for failed messages:

```go
type Config struct {
    ProjectID      string
    TopicID        string
    SubscriptionID string
    DLQTopicID     string   // NEW: separate dead letter topic
    MaxAttempts    int      // NEW: max delivery attempts (default: 10)
    MaxMessages    int
    TopicAttribute string
}
```

Flow:
1. Publish includes `X-Attempts` attribute
2. Poller receives, parses Attempts
3. If Attempts > MaxAttempts → publish to DLQ topic, Ack original
4. Otherwise → normal processing
5. Handler returns DLQ → `SendToDLQ()` publishes to DLQ topic, Acks original

#### Approach B: Subscription Policy (Alternative)

Rely on Pub/Sub subscription-level DLQ policy:
1. Publish includes `X-Attempts`
2. Subscription configured with max delivery attempts + DLQ topic
3. When handler returns DLQ → Nack (triggers redelivery)
4. After max deliveries → Pub/Sub automatically routes to DLQ topic
5. No `DLQTopicID` in go-task-orbit config

**Decision: Approach A** — more predictable, matches SQS behavior, no dependency on subscription configuration.

---

### Key Risks

| Risk | Mitigation |
|---|---|
| DLQ topic must be pre-provisioned | Same as SQS — documented requirement. Library does not create/manage topics. |
| `X-Attempts` attribute format | Store as integer string (`"3"`). Simple, human-readable in Pub/Sub console. |
| Backward compatibility | Old messages without `X-Attempts` default to Attempts=0 (implicit). No migration needed. |

---

### Assumptions

1. DLQ topic is pre-provisioned and accessible to the service account / Workload Identity
2. `go-task-orbit` topic/subscription are already created before pipeline starts
3. Pub/Sub emulator supports `CreateTopic` for e2e test setup (Google emulator does)

---

### Out of Scope

- Auto-provisioning of DLQ topics
- Subscription-level DLQ policy configuration
- Multi-topic routing within Pub/Sub

---

### Confirmation

- [x] Business goal: Stop infinite Nack loops in Pub/Sub transport
- [x] Systems impacted: `transport/pubsub/`, e2e tests
- [x] Change approach: Persist Attempts in attributes + DLQ topic routing
- [x] Scope boundaries: Transport-level fix only — no changes to ringq runtime
