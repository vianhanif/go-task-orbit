## IO-359: go-task-orbit — E2E Tests with Floci + GitHub Actions

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | E2E tests using Floci (AWS emulator) + GitHub Actions CI |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Go Version | 1.21+ |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Add end-to-end integration tests that exercise the full pipeline — SQS transport → ring buffer → worker pool → ack/retry/DLQ — using [Floci](https://floci.io/) as a local AWS emulator. Wire them into GitHub Actions for CI.

#### Why it is needed
Current tests use the in-memory transport (`transport/memory`), which bypasses the SQS transport entirely. The SQS transport has unit tests (`transport/sqs/sqs_test.go`) but no integration tests that verify the full pipeline with actual SQS protocol interactions. Floci provides a fast (24ms startup), no-auth, MIT-licensed SQS emulator that can run in GitHub Actions without cloud credentials.

#### Success criteria
- [ ] Docker-based Floci AWS emulator running SQS locally and in CI
- [ ] E2E test: publish → receive → process → ack (happy path)
- [ ] E2E test: publish → receive → process → retry → ack
- [ ] E2E test: publish → receive → process → DLQ
- [ ] E2E test: idempotency with real SQS message attributes
- [ ] E2E test: batch processing (multiple messages in one receive)
- [ ] E2E test: visibility timeout extension via Nack/RetryWithDelay
- [ ] E2E test: graceful shutdown drains inflight messages
- [ ] GitHub Actions workflow that starts Floci, runs e2e tests
- [ ] Tests are skipped if Floci is not available (for local dev without Docker)

---

### Architecture

```
Local Development                              CI (GitHub Actions)
┌──────────────────────┐              ┌──────────────────────────────┐
│ task e2e             │              │ task e2e-ci                  │
│   └─ start floci     │              │   ├─ go test -tags=e2e       │
│   └─ go test -tags=e2e│             │   └─ (Floci via svc container)│
│   └─ stop floci      │              └──────────────────────────────┘
└──────────────────────┘
         │
         └── Taskfile.yml (single source of truth for all commands)
```

---

#### Test Flow (each e2e test)

```
1. Create SQS queue via AWS SDK (endpoint: http://localhost:4566)
2. Create DLQ queue
3. Configure SQS transport with queue URLs
4. Build pipeline: SQS transport → handler → result
5. Publish message to SQS (via transport or direct SDK)
6. Run pipeline
7. Assert: handler was called, message was acked, DLQ received (if expected)
8. Delete queues
```

---

### Scope Table

| # | Scope | Details | Complexity | Recommended LLM | Estimate |
|---|---|---|---|---|---|
| 1 | Taskfile.yml | Single source of truth for test/lint/e2e commands. Replaces raw Docker/shell scripts. | Low | Fast | 0.5h |
| 2 | Floci setup helper | Go test helper that starts/pings Floci, creates queues, provides teardown | Low | Fast | 1h |
| 3 | E2E test: happy path | Publish → receive → process → ack | Medium | Mid | 1h |
| 4 | E2E test: retry + ack | Handler fails once, succeeds on retry. Verify message is acked | Medium | Mid | 1h |
| 5 | E2E test: DLQ | Handler returns DLQ action. Verify message appears in DLQ queue | Medium | Mid | 1h |
| 6 | E2E test: idempotency | Two messages with same IdempotencyKey. Verify handler called once, duplicate acked | Medium | Mid | 1h |
| 7 | E2E test: batch receive | Publish N messages, verify all processed by handler(s) | Low | Fast | 0.5h |
| 8 | E2E test: RetryWithDelay | Handler returns RetryWithDelay, verify visibility timeout extended | Medium | Mid | 1h |
| 9 | E2E test: graceful shutdown | Publish slow message, cancel context, verify inflight drains | Medium | Mid | 1h |
| 10 | GitHub Actions workflow | `.github/workflows/e2e.yml` — uses Task, Floci service container | Low | Fast | 0.5h |
| 11 | Test tags + skip | Build tag `e2e` to separate from unit tests; `t.Skip()` if Floci unreachable | Low | Fast | 0.5h |

**Total estimate:** ~9.5h

---

### Package Structure (new)

```
go-task-orbit/
├── Taskfile.yml                  # NEW: task runner (test, lint, e2e, floci-start/stop)
├── .github/
│   └── workflows/
│       └── e2e.yml              # GitHub Actions: Floci service, Task-based
├── e2e/
│   ├── e2e_test.go              # TestMain: setup/teardown, queue creation
│   ├── happy_path_test.go       # Basic publish → ack
│   ├── retry_test.go            # Retry → ack
│   ├── dlq_test.go              # DLQ routing
│   ├── idempotency_test.go      # Deduplication
│   ├── batch_test.go            # Batch processing
│   ├── visibility_test.go       # RetryWithDelay / visibility timeout
│   ├── shutdown_test.go         # Graceful shutdown
│   └── helper.go                # Floci helpers: create queue, purge, delete
```

All files use `//go:build e2e` tag.

---

#### Taskfile.yml (replaces raw Docker/shell commands)

```yaml
# Taskfile.yml — single source of truth for all test/lint/e2e commands
version: '3'

vars:
  FLOCI_IMAGE: floci/floci:latest
  FLOCI_PORT: '4566'

tasks:
  test:
    desc: Run unit tests
    cmds:
      - go test -count=1 -timeout=60s ./...

  test-race:
    desc: Run unit tests with race detector
    cmds:
      - go test -race -count=1 ./...

  lint:
    desc: Run go vet
    cmds:
      - go vet ./...

  e2e:
    desc: Run e2e tests with Floci (local Docker)
    cmds:
      - task: floci-start
      - defer: { task: floci-stop }
      - go test -tags=e2e -v -count=1 -timeout=120s ./e2e/...

  e2e-ci:
    desc: Run e2e tests in CI (Floci already running as service container)
    cmds:
      - go test -tags=e2e -v -count=1 -timeout=120s ./e2e/...

  floci-start:
    desc: Start Floci AWS emulator
    cmds:
      - docker run -d --name floci-e2e -p {{.FLOCI_PORT}}:{{.FLOCI_PORT}} {{.FLOCI_IMAGE}}
      - |
        until curl -s http://localhost:{{.FLOCI_PORT}}/_floci/health; do
          sleep 0.5
        done

  floci-stop:
    desc: Stop and remove Floci container
    cmds:
      - docker rm -f floci-e2e 2>/dev/null || true
    ignore_error: true

  all:
    desc: Run lint + unit tests + e2e tests
    cmds:
      - task: lint
      - task: test
      - task: e2e
```

#### GitHub Actions Workflow (uses Task)

```yaml
# .github/workflows/e2e.yml
name: E2E Tests

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  e2e:
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
```

---

### Floci Setup Helper (design sketch)

```go
// e2e/helper.go
//go:build e2e

package e2e

import (
    "context"
    "os"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

const (
    flociEndpoint = "http://localhost:4566"
    mainQueueName = "e2e-main"
    dlqQueueName  = "e2e-dlq"
)

func setupQueues(t *testing.T) (mainURL, dlqURL string, client *sqs.Client) {
    t.Helper()
    // Load AWS config with endpoint override pointing at Floci
    cfg, err := config.LoadDefaultConfig(context.Background(),
        config.WithRegion("us-east-1"),
    )
    // Override endpoint
    client = sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.BaseEndpoint = aws.String(flociEndpoint)
    })
    // Create queues
    // ...
    return mainURL, dlqURL, client
}
```

---

### Key Risks

| Risk | Mitigation |
|---|---|
| Floci Docker image pull rate limit | GitHub Actions runner caches Docker layers; use `docker pull` with retry |
| Floci SQS vs real SQS behavior differences | Test both happy path and edge cases; document any known discrepancies |
| Test speed (Docker startup ~24ms, queue creation ~100ms) | Pool queues across tests via TestMain; acceptable for e2e suite |
| Go 1.21+ compatibility with Floci SDK interaction | AWS SDK v2 works fine; endpoint override is standard SDK feature |
| Local dev without Docker | `t.Skip()` if Floci is unreachable; unit tests still pass via in-memory transport |

---

### Assumptions

1. Floci `floci/floci:latest` Docker image includes SQS (confirmed — 47 AWS services)
2. SQS endpoint override via `BaseEndpoint` works with AWS SDK Go v2
3. GitHub Actions Ubuntu runner has Docker available
4. Queue creation/deletion via SDK is fast enough for per-test isolation
5. Tests run sequentially (not parallel) to avoid queue state conflicts

---

### Out of Scope

- Azure e2e tests (separate task)
- GCP e2e tests (covered in `20260531-IO-359-gcp-pubsub-transport.md`)
- Performance/load testing
- Chaos testing (network partitions, Floci crashes)
- Integration with real AWS SQS (requires cloud credentials)

---

### Confirmation

- [x] Business goal: Verify full pipeline works end-to-end with real SQS protocol
- [x] Systems impacted: New `e2e/` test package, new GitHub Actions workflow
- [x] Change approach: Floci Docker in CI, build-tag-separated e2e tests
- [x] Scope boundaries: SQS-only e2e tests, no real AWS, no performance testing
