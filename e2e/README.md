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
| 7 | UnknownTopic | SQS, Pub/Sub | Message to unregistered topic → OnError hook fired |

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
    ✅ published: hello → handler called, acked
--- PASS: TestE2EHappyPath (2.51s)
=== RUN   TestE2ERetryThenAck
    ✅ published: retry-me → failed (attempt 1) → retried → succeeded (attempt 2)
--- PASS: TestE2ERetryThenAck (5.51s)
=== RUN   TestE2EDLQ
    ✅ published: fail → handler returned DLQ → OnError fired → sent to DLQ
--- PASS: TestE2EDLQ (2.51s)
=== RUN   TestE2EIdempotency
    ✅ published 2 messages (same key) → handler called once, duplicate acked
--- PASS: TestE2EIdempotency (4.51s)
=== RUN   TestE2EBatchReceive
    ✅ published 5 messages → all 5 processed
--- PASS: TestE2EBatchReceive (3.51s)
=== RUN   TestE2EGracefulShutdown
    ✅ published slow → cancel → inflight drained → handler completed
--- PASS: TestE2EGracefulShutdown (2.01s)
=== RUN   TestE2EUnknownTopic
    ✅ published to unknown topic → OnError fired → routed to DLQ
--- PASS: TestE2EUnknownTopic (2.51s)
```

### GCP Pub/Sub (Google Emulator)

```
=== RUN   TestE2EGCPHappyPath
    ✅ published: hello-gcp → handler called, acked
--- PASS: TestE2EGCPHappyPath (5.61s)
=== RUN   TestE2EGCPRetry
    ✅ published: retry-me → failed (attempt 1) → retried → succeeded (attempt 2)
--- PASS: TestE2EGCPRetry (8.53s)
=== RUN   TestE2EGCPDLQ
    ✅ published: fail → handler returned DLQ → OnError fired → Nacked
--- PASS: TestE2EGCPDLQ (5.52s)
=== RUN   TestE2EGCPIdempotency
    ✅ published 2 messages (same key) → handler called once, duplicate filtered
--- PASS: TestE2EGCPIdempotency (8.53s)
=== RUN   TestE2EGCPBatch
    ✅ published 5 messages → all 5 processed
--- PASS: TestE2EGCPBatch (5.57s)
=== RUN   TestE2EGCPUnknownTopic
    ✅ published to unknown topic → OnError fired → Nacked
--- PASS: TestE2EGCPUnknownTopic (5.52s)
```

## Adding New E2E Tests

1. Use build tag `//go:build e2e` for SQS, `//go:build e2e_gcp` for Pub/Sub
2. Call `setupEnv(t)` (SQS) or `setupPubSubEnv(t)` (Pub/Sub) for emulator setup
3. Create transport via `newSQSTransport(queueURL)` or `env.createTransport(t)`
4. Build pipeline with test handler
5. Call `t.Log()` for test progress (visible in CI logs)
6. Call `env.cleanup(t)` at the end
