# E2E Tests

End-to-end tests that exercise the full go-task-orbit pipeline against real message broker protocols using cloud emulators.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    E2E Test Suite                        │
├────────────────────────┬────────────────────────────────┤
│  AWS SQS (Floci :4566) │  GCP Pub/Sub (Emulator :8085)  │
│  ───────────────────── │  ───────────────────────────── │
│  Build tag: e2e        │  Build tag: e2e_gcp            │
│  CI job: e2e-aws       │  CI job: e2e-gcp              │
└────────────────────────┴────────────────────────────────┘
```

Both transports share the same test scenarios. Each test:
1. Creates a queue/topic + subscription on the emulator
2. Builds a pipeline: Transport → Ring Buffer → Worker Pool
3. Publishes test message(s)
4. Verifies handler behavior (acks, retries, DLQ, idempotency)
5. Cleans up

## Test Scenarios

| # | Test | Transport | What it verifies |
|---|---|---|---|
| 1 | HappyPath | SQS, Pub/Sub | Message published → received → handler called → acked |
| 2 | RetryThenAck | SQS, Pub/Sub | Handler fails once → retried → succeeds on second attempt |
| 3 | DLQ | SQS, Pub/Sub | Handler returns DLQ → error hook fired → message routed to DLQ |
| 4 | Idempotency | SQS, Pub/Sub | Two messages with same key → handler called once, duplicate filtered |
| 5 | BatchReceive | SQS, Pub/Sub | N messages published → all N processed by handler(s) |
| 6 | GracefulShutdown | SQS | Slow handler in-flight → cancel context → handler completes before exit |
| 7 | UnknownTopic | SQS, Pub/Sub | Message to unregistered topic → OnError hook fired → routed to DLQ |
| 8 | ETADelayedTask | SQS, Pub/Sub | Message with NotBefore=3s → held in timer wheel → processed after delay |
| 9 | ETAImmediateTask | SQS | Message with NotBefore=0 → processed immediately |
| 10 | ETAExponentialBackoff | SQS | Handler returns Retry repeatedly → delays grow exponentially

## Run Locally

```bash
# AWS SQS e2e (requires Docker)
task e2e

# GCP Pub/Sub e2e (requires Docker)
task e2e-gcp

# Both
task e2e-all
```

Emulators are started/stopped automatically via Taskfile.yml.

## CI Results

Latest run: **2026-05-30** — [All passing](https://github.com/vianhanif/go-task-orbit/actions)

### AWS SQS (Floci)

```
=== RUN   TestE2EHappyPath
    ✅ pipeline started → published → handler called → acked
--- PASS: TestE2EHappyPath (5.51s)
=== RUN   TestE2ERetryThenAck
    ✅ published → handler failed (attempt 1) → retried → succeeded (attempt 2)
--- PASS: TestE2ERetryThenAck (8.51s)
=== RUN   TestE2EDLQ
    ✅ published → handler returned DLQ → OnError fired → sent to DLQ
--- PASS: TestE2EDLQ (5.51s)
=== RUN   TestE2EIdempotency
    ✅ published 2 messages (same key) → handler called once, duplicate acked
--- PASS: TestE2EIdempotency (8.51s)
=== RUN   TestE2EBatchReceive
    ✅ published 5 messages → all 5 processed
--- PASS: TestE2EBatchReceive (5.51s)
=== RUN   TestE2EGracefulShutdown
    ✅ published slow message → cancel → inflight drained → handler completed
--- PASS: TestE2EGracefulShutdown (4.01s)
=== RUN   TestE2EUnknownTopic
    ✅ published to unknown topic → OnError fired → routed to DLQ
--- PASS: TestE2EUnknownTopic (5.52s)
=== RUN   TestE2EETADelayedTask
    ✅ published with NotBefore=3s → held in timer → processed after delay
--- PASS: TestE2EETADelayedTask (6.52s)
=== RUN   TestE2EETAImmediateTask
    ✅ published with NotBefore=0 → processed immediately
--- PASS: TestE2EETAImmediateTask (3.51s)
=== RUN   TestE2EETAExponentialBackoff
    ✅ published → retries with growing delay → DLQ after max attempts
--- PASS: TestE2EETAExponentialBackoff (30.51s)
```

### GCP Pub/Sub (Google Emulator)

```
=== RUN   TestE2EGCPHappyPath
    ✅ published → handler called → acked
--- PASS: TestE2EGCPHappyPath (5.52s)
=== RUN   TestE2EGCPRetryThenAck
    ✅ published → handler failed (attempt 1) → retried → succeeded (attempt 2)
--- PASS: TestE2EGCPRetryThenAck (8.52s)
=== RUN   TestE2EGCPDLQ
    ✅ published → handler returned DLQ → OnError fired → Nacked
--- PASS: TestE2EGCPDLQ (5.52s)
=== RUN   TestE2EGCPIdempotency
    ✅ published 2 messages (same key) → handler called once, duplicate filtered
--- PASS: TestE2EGCPIdempotency (8.53s)
=== RUN   TestE2EGCPBatchReceive
    ✅ published 5 messages → all 5 processed
--- PASS: TestE2EGCPBatchReceive (5.56s)
=== RUN   TestE2EGCPUnknownTopic
    ✅ published to unknown topic → OnError fired → Nacked
--- PASS: TestE2EGCPUnknownTopic (5.52s)
=== RUN   TestE2EGCPETADelayedTask
    ✅ published with NotBefore=3s → held in timer → processed after delay
--- PASS: TestE2EGCPETADelayedTask (6.62s)
=== RUN   TestE2EGCPETABackoff
    ⏭ skipped — Pub/Sub Nack redelivery resets attempt counter
--- SKIP: TestE2EGCPETABackoff (0.00s)
```

## Adding New E2E Tests

1. Use build tag `//go:build e2e` for SQS, `//go:build e2e_gcp` for Pub/Sub
2. Call `setupEnv(t)` (SQS) or `setupPubSubEnv(t)` (Pub/Sub) for emulator setup
3. Create transport via `newSQSTransport(queueURL)` or `env.createTransport(t)`
4. Build pipeline with test handler
5. Call `t.Log()` for test progress (visible in CI logs)
6. Call `env.cleanup(t)` at the end
