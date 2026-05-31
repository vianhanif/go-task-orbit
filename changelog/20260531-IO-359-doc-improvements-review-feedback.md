## IO-359: go-task-orbit — Documentation Improvements (ChatGPT Review Feedback)

### Ticket Info

| Field | Value |
|---|---|
| Ticket ID | IO-359 |
| Type | Story |
| Title | Documentation improvements from external review |
| Repo | `github.com/vianhanif/go-task-orbit` |
| Branch source | `main` |

---

### Task Overview

#### What is changing
Update README and add documentation to address gaps identified in an external review. The feedback focused on missing explainers for architecture decisions, concurrency model, ordering semantics, backpressure, and comparative positioning.

#### Why it is needed
Current docs describe *what* the library does but not *why* the architecture choices exist. Engineers evaluating the library need to understand: ring buffer rationale, concurrency guarantees, ordering semantics, retry behavior, idempotency failure windows, and how it compares to alternatives.

---

### Scope Table

| # | Scope | Details | Complexity | Estimate |
|---|---|---|---|---|
| 1 | "Why Ring Buffer" section | Explain benefits over channels/goroutine-per-msg: allocations, cache locality, bounded throughput, overflow policies | Low | 0.3h |
| 2 | Better positioning | Change tagline from "async worker library" to "cloud-native async execution runtime" | Low | 0.1h |
| 3 | Concurrency model section | What runs concurrently vs serialized: transport intake, ring dispatch, worker execution | Low | 0.3h |
| 4 | Parallel execution section | Clarify N workers run handlers in parallel across cores, bounded by WorkerCount | Low | 0.2h |
| 5 | Ordering semantics | Explicit: "messages may process out of order across workers; FIFO not guaranteed" | Low | 0.1h |
| 6 | Backpressure section | Explain Block/DropNewest/DropOldest/Reject, ring buffer sizing, KEDA scaling | Low | 0.3h |
| 7 | Comparison table | vs go-workers, Asynq, Temporal, Watermill, Go channels | Medium | 0.5h |
| 8 | Retry docs expansion | Visibility timeout interaction, exponential backoff default, DLQ timing, transport-specific behavior | Medium | 0.5h |
| 9 | Idempotency failure windows | Crash-after-side-effect, duplicate delivery scenarios, at-least-once implications | Low | 0.3h |
| 10 | Split /docs/ folder | architecture.md, transports.md, retries.md, idempotency.md, concurrency.md, comparison.md | High | 2h |
| 11 | Benchmarks section | Throughput, allocations/op, p95 latency from bench/ results in README | Medium | 1h |
| 12 | Per-key concurrency | `ConcurrencyKey` — same key serialized, different keys parallel (new feature, future) | N/A | deferred |

**Total estimate (items 1-9, 11):** ~4h

### Implementation Approach

Items 1-6 and 9 are README additions. Items 7-8 require research for accurate comparisons. Item 10 is a larger restructuring. Item 11 requires running benchmarks first.

**Phase A (this session):** Items 1-6, 9 — README sections (Quick wins)
**Phase B (this session):** Items 7-8 — Comparison + retry expansion
**Phase C (future):** Items 10-11 — Docs split + benchmarks
**Phase D (future):** Item 12 — New feature

---

### Confirmation

- [x] Source: External ChatGPT review of repo docs
- [x] Changes: README additions and new /docs/ files
- [x] No code changes — documentation only
