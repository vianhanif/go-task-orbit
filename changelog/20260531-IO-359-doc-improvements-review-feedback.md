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
| 13 | Worker lifecycle docs | Clarify: one worker = one goroutine, handlers not reused, retry re-enqueues | Low | 0.3h |
| 14 | Ring buffer bottleneck note | Document single-consumer dispatch may bottleneck before workers saturate | Low | 0.2h |
| 15 | Transport latency smoothing | Highlight ring buffer as SQS/PubSub burst absorber — more important than raw throughput | Low | 0.2h |
| 16 | Reposition: "predictable bounded parallel execution" | Avoid overselling "lock-free"/"high-performance". Position as predictable, not fastest. | Low | 0.1h |
| 17 | Flow diagrams | Retry → DLQ lifecycle, shutdown draining sequence, receive loop | Medium | 1h |

**Total estimate (items 1-9, 11):** ~3.5h (completed)

### Implementation Status

**Done (17 of 21):**
- README: added Why Ring Buffer, Concurrency Model, Backpressure, Comparison, Retry & DLQ, Idempotency failure windows
- README: updated tagline to "cloud-native async execution runtime"
- RING-BUFFER.md: replaced conceptual perf claims with actual benchmark results
- bench/: added Go channel and goroutine-per-message comparisons

**Remaining (4 of 21):**
- Item 10: Split /docs/ folder (deferred)
- Item 12: Per-key concurrency feature (deferred)
- Item 17: Flow diagrams (deferred — needs visual work)
- Items 1-16, 18-21: Complete

### Phase Summary

| Phase | Items | Status |
|---|---|---|
| A | 1-6, 9, 13-17 | Items 1-6,9 done. Items 13-17 pending. |
| B | 7-8 | Complete |
| C | 10-11 | Item 11 done, item 10 deferred |
| D | 12 | Deferred |

---

### Confirmation

- [x] Source: External ChatGPT review of repo docs
- [x] Changes: README additions and new /docs/ files
- [x] No code changes — documentation only

---

### Progress Checklist

| # | Item | Status | Phase |
|---|---|---|---|
| 1 | "Why Ring Buffer" section | Done | A |
| 2 | Better positioning (tagline) | Done | A |
| 3 | Concurrency model section | Done | A |
| 4 | Parallel execution section | Done | A |
| 5 | Ordering semantics | Done | A |
| 6 | Backpressure section | Done | A |
| 7 | Comparison table | Done | B |
| 8 | Retry docs expansion | Done | B |
| 9 | Idempotency failure windows | Done | A |
| 10 | Split /docs/ folder | Pending | C |
| 11 | Benchmarks section | Done | C |
| 12 | Per-key concurrency | Deferred | D |
| 13 | Worker lifecycle docs | Done | A |
| 14 | Ring buffer bottleneck note | Done | A |
| 15 | Transport latency smoothing | Done | A |
| 16 | Reposition: predictable bounded execution | Done | A |
| 17 | Flow diagrams | Pending | A |
| 18 | Ordering guarantees section (2nd review) | Done | A |
| 19 | Ring buffer implementation details (2nd review) | Done | A |
| 20 | Parallelism tuning guidance table (2nd review) | Done | A |
| 21 | Benchmark summary in README (2nd review) | Done | A |

---

## Second Review (post-update)

After initial doc fixes, ChatGPT re-reviewed. Scores improved significantly.

### Score improvements

| Area | Before | After |
|---|---|---|
| Concurrency explanation | 6/10 | **8.5/10** |
| Parallelism explanation | 5/10 | **8/10** |
| Runtime clarity | 6.5/10 | **8.5/10** |
| Operational credibility | 7/10 | **8.5/10** |
| Architecture positioning | 7/10 | **8.5/10** |
| Ordering semantics | 4/10 | 5/10 (still weak) |
| Benchmark storytelling | 4/10 | 4/10 (unchanged) |

### New gaps identified

1. **Ordering guarantees still unclear** — explicit section needed: "messages may execute out-of-order across workers, retries can reorder, same-entity messages can race"
2. **Ring buffer implementation underspecified** — readers ask: mutex or atomic? MPSC or MPMC? blocking or non-blocking?
3. **Parallelism tuning guidance missing** — WorkerCount for CPU-heavy (GOMAXPROCS) vs IO-heavy (32-128), RingBufferSize for burst absorption
4. **Benchmarks not surfaced** — throughput table per worker count should be in README for instant credibility
5. **Architecture still undersold** — docs describe features but don't frame the runtime as "bounded async execution runtime"
