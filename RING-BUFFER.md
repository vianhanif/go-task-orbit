# Ring Buffer

## What is a Ring Buffer?

A ring buffer (also called a circular buffer) is a fixed-size data structure that wraps around — when you reach the end, you go back to the beginning. Think of it like a conveyor belt in a factory: items go on at one end, get picked off at the other, and the belt keeps looping.

```
   write here ──→ ┌───┬───┬───┬───┬───┬───┬───┬───┐
                   │ A │ B │ C │   │   │   │   │   │
                   └───┴───┴───┴───┴───┴───┴───┴───┘
                                 ↑── read here

   After wrapping:

                   ┌───┬───┬───┬───┬───┬───┬───┬───┐
   write here ──→  │ X │ B │ C │ D │ E │ F │ G │ H │
                   └───┴───┴───┴───┴───┴───┴───┴───┘
                     ↑── read here
```

The key property: **it never grows**. The size is fixed at creation time. This boundedness is the foundation for predictable latency, backpressure control, and memory safety.

---

## Why a Ring Buffer Instead of...

### ...Go Channels?

Channels are the idiomatic Go concurrency primitive, but they have limitations for high-throughput worker systems:

```mermaid
flowchart LR
    subgraph "Go Channel"
        P1[Producer 1] -->|"ch <- msg"| CH{Unbuffered<br/>Channel} -->|"msg := <-ch"| W1[Worker 1]
        P2[Producer 2] -->|"ch <- msg"| CH
        P3[Producer 3] -->|"ch <- msg"| CH
        CH -.->|"blocks when<br/>receiver slow"| P1
    end
```

| Problem | Channel | Ring Buffer |
|---|---|---|
| Fan-out to multiple workers | One receiver per channel — needs extra orchestration | Multiple consumers can read different slots simultaneously |
| Backpressure policy | Blocks producer (or panics on full unbuffered) | Configurable: Block, DropNewest, DropOldest, Reject |
| Batch reads | Read one at a time; batching requires accumulation logic | Read N slots in one pass — zero-copy batch drain |
| Cache locality | Values move through heap — pointer chasing | Contiguous memory — CPU cache-friendly |
| Visibility | `len(ch)` is approximate, no built-in metrics | Head/tail are atomics — exact queue depth, throughput, drops |
| Memory | Unbounded buffered channels can grow indefinitely | Fixed allocation, no GC pressure under load |

### ...Direct Goroutine Per Message?

```mermaid
flowchart TB
    subgraph "Goroutine Per Message"
        SQS[SQS Poller] -->|"spawn goroutine"| G1[goroutine 1]
        SQS -->|"spawn goroutine"| G2[goroutine 2]
        SQS -->|"spawn goroutine"| G3[goroutine 3]
        SQS -->|"spawn goroutine"| GN[goroutine ...N]
        G1 --> CPU[CPU Thrashing]
        G2 --> CPU
        G3 --> CPU
        GN --> CPU
    end
```

Under load, this causes:
- **Goroutine explosion** — thousands of goroutines, each with its own stack (min 2KB)
- **CPU thrashing** — scheduler spends more time context-switching than doing work
- **No backpressure** — SQS keeps delivering, system keeps spawning, memory grows unbounded
- **Visibility timeout chaos** — goroutines take too long to start, messages become visible again, duplicates appear

### ...Redis Polling (go-workers style)?

```mermaid
flowchart LR
    subgraph "Redis Polling"
        P[Producer] -->|"LPUSH"| R[(Redis)]
        W1[Worker 1] -->|"BRPOP (poll)"| R
        W2[Worker 2] -->|"BRPOP (poll)"| R
        W3[Worker 3] -->|"BRPOP (poll)"| R
    end
```

Problems:
- **Network hop on every message** — adds 1-5ms latency per job
- **Redis is a bottleneck** — all workers contend on the same Redis instance
- **Polling is wasteful** — constant `BRPOP` calls even when queue is empty
- **Redis coupling** — can't run or test without Redis
- **Lock contention** — atomic list operations still serialize access at Redis level
- **No batching** — one message per round-trip (unless using pipelines, which adds complexity)

---

## How the Ring Buffer Works in go-task-orbit

### The Flow

```mermaid
sequenceDiagram
    participant SQS as SQS Poller
    participant RB as Ring Buffer
    participant DP as Dispatcher
    participant WP as Worker Pool
    participant AB as Ack Batcher

    SQS->>SQS: batch ReceiveMessage (up to 10)
    SQS->>RB: EnqueueBatch(msgs)
    Note over RB: write messages to slots<br/>advance head (atomic)
    
    DP->>RB: ReadBatch()
    Note over RB: read N slots<br/>advance tail (atomic)
    DP->>WP: SubmitBatch(msgs)
    
    WP->>WP: bounded workers process
    WP->>AB: collect results (Ack/Retry/DLQ)
    
    AB->>SQS: DeleteMessageBatch
    AB->>SQS: SendMessage (DLQ, on failure)
```

### Inside the Ring Buffer

```mermaid
flowchart TB
    subgraph "Ring Buffer Internals"
        direction LR
        
        subgraph "Write Side (Producer)"
            H[Head Cursor<br/>atomic.Uint64]
        end
        
        subgraph "Slots"
            S0["Slot 0<br/>seq: 42<br/>msg ref"]
            S1["Slot 1<br/>seq: 43<br/>msg ref"]
            S2["Slot 2<br/>seq: 44<br/>msg ref"]
            S3["Slot 3<br/>empty"]
            SD["..."]
        end
        
        subgraph "Read Side (Consumer)"
            T[Tail Cursor<br/>atomic.Uint64]
        end
        
        H -->|"write at<br/>head & mask"| S0
        T -->|"read at<br/>tail & mask"| S2
    end
```

**The key trick: power-of-two sizing.**

```go
bufferSize := 4096       // must be power of 2
mask := bufferSize - 1   // 4095 = 0b111111111111
index := sequence & mask // cheap modulo — no division
```

This makes index calculation a single bitwise AND — orders of magnitude faster than `%`.

### Memory Barrier Strategy

The ring buffer synchronizes producers and consumers without locks using atomic operations:

```
Producer:                          Consumer:
  1. Write payload to slot           1. Read head cursor (atomic)
  2. StoreFence (memory barrier)     2. LoadFence (memory barrier)
  3. Publish slot sequence (atomic)  3. Read payload from slot
                                     4. Advance tail (atomic)
```

In Go, this maps to:

```go
// Producer
slot.payload = msg          // plain write
atomic.StoreUint64(&slot.sequence, seq)  // publish — acts as StoreFence

// Consumer
seq := atomic.LoadUint64(&slot.sequence) // acquire — acts as LoadFence
msg := slot.payload         // plain read (safe — sequence guarantees visibility)
```

The atomic store/load on `sequence` serves double duty: it tracks position AND acts as the memory barrier that guarantees the payload write is visible to consumers.

### Backpressure Policies

When the ring is full and a producer tries to write:

```
Buffer: [A][B][C][D][E][F][G][H]  ← FULL (8/8 slots used)
         ↑read                    ↑write
```

```mermaid
flowchart TD
    P[Producer: Enqueue msg] --> Q{Buffer full?}
    Q -->|No| W[Write to slot]
    Q -->|Yes| POL{Overflow Policy}
    POL -->|Block| BL[Wait for consumer<br/>to free a slot]
    POL -->|DropNewest| DN[Discard incoming msg<br/>increment drop counter]
    POL -->|DropOldest| DO[Overwrite oldest slot<br/>advance tail]
    POL -->|Reject| RJ[Return error to caller]
```

This is something Go channels cannot do — their only option is to block (or panic on unbuffered).

---

## Performance Comparison

### Throughput (messages/sec) — conceptual

```
Ring Buffer (batch):     ████████████████████████████████  ~500K msg/s
Go Channel (single):     ██████████████                    ~200K msg/s
Redis List (BRPOP):      ████                              ~50K msg/s
Goroutine per msg:       ██                                ~30K msg/s
MySQL polling:           █                                 ~5K msg/s
```

The ring buffer wins because:
1. **No syscalls** — all in-process, no kernel transitions
2. **No network** — no TCP handshake, no serialization
3. **No heap allocation per message** — slots are pre-allocated, messages are references
4. **Batch operations** — drain N messages in one atomic operation
5. **Cache locality** — contiguous memory means CPU prefetcher works

### Latency Distribution

```
Ring Buffer:  P50=2μs   P99=8μs   P999=15μs  (tight — no outliers)
Channel:      P50=5μs   P99=50μs  P999=200μs (scheduler jitter)
Redis:        P50=1ms   P99=5ms   P999=20ms  (network jitter)
```

The ring buffer's bounded, lock-minimized design produces **predictable latency** — critical for systems where tail latency matters (API backends, payment processing).

---

## The Big Picture: Where Ring Buffer Fits

```mermaid
flowchart TB
    subgraph "Across the Network (slow, durable)"
        SQS[Amazon SQS<br/>Durability, Cross-Pod, HA]
    end

    subgraph "Inside the Process (fast, predictable)"
        RB[Ring Buffer<br/>Scheduling, Batching,<br/>Backpressure]
        WP[Worker Pool<br/>Bounded Concurrency]
        AB[Ack Batcher<br/>Batch DeleteMessages]
    end

    subgraph "External (optional, pluggable)"
        ID[Idempotency Store<br/>In-Memory / Redis / DB]
        MT[Metrics / Traces<br/>via Hooks → OTel]
    end

    SQS -->|"batch receive<br/>~10ms latency"| RB
    RB -->|"batch dispatch<br/>~2μs latency"| WP
    WP -->|"collect results"| AB
    AB -->|"batch delete<br/>~5ms latency"| SQS

    RB -.->|"check dedupe"| ID
    WP -.->|"emit hooks"| MT
```

**The design principle:**

> SQS handles what's hard about distributed systems (durability, delivery, HA).
> The ring buffer handles what's hard about local execution (scheduling, concurrency, backpressure).
> Neither does the other's job.

This separation is why the system stays fast AND reliable — the slow network operations (SQS) are batched and decoupled from the fast in-process operations (ring dispatch).

---

## When NOT to Use a Ring Buffer

Ring buffers excel at throughput and predictability, but they're wrong for:

| Scenario | Why Not | Alternative |
|---|---|---|
| Unbounded queue growth needed | Ring is fixed-size; can't grow | Redis list, Kafka |
| Payloads > a few KB | Large payloads waste cache, cause allocation | Store payloads externally (S3), pass refs in ring |
| Fewer than thousands of msg/sec | Overhead not worth it; channels are simpler | Go channels |
| Strict global ordering across pods | Ring is per-process; no cross-pod ordering | Kafka partitions, SQS FIFO |
| Persistent queue across restarts | Ring is in-memory, lost on crash | SQS, Redis, Kafka (already the transport layer) |
| Single-digit goroutines | Ring's batching advantage requires volume | Direct goroutine spawn |

---

## Summary

| Property | How the Ring Buffer Achieves It |
|---|---|
| **High throughput** | Batch operations, no syscalls, no heap allocation per msg |
| **Low, predictable latency** | Lock-minimized (atomics), cache-friendly, no GC pressure |
| **Backpressure control** | Fixed size + configurable overflow policies |
| **Memory safety** | Pre-allocated, bounded — no unbounded growth |
| **Visibility** | Atomic head/tail give exact queue depth, drops, throughput |
| **Testability** | No external dependencies — fast unit tests, deterministic behavior |

The ring buffer is not a replacement for SQS — it's the turbocharger that sits between SQS and your workers, turning batch network receives into microsecond-local dispatch.
