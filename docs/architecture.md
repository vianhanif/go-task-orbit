# Architecture

go-task-orbit is a cloud-native async execution runtime with ring-buffer scheduling and pluggable transports.

## Pipeline

```mermaid
flowchart LR
    Producer -->|"Publish"| Transport
    Transport -->|"Receive"| Poller
    Poller -->|"Enqueue"| Ring[Ring Buffer]
    Ring -->|"Dequeue"| Dispatcher
    Dispatcher -->|"Filter"| Idem[Idempotency]
    Idem -->|"Dispatch"| Workers[Worker Pool]
    Workers -->|"Ack"| Transport
    Workers -->|"Retry"| Ring
    Workers -->|"DLQ"| Transport
```

> Rendered: [diagram-pipeline.svg](diagram-pipeline.svg)

## Receive Loop

```mermaid
sequenceDiagram
    participant T as Transport
    participant P as Poller
    participant R as Ring Buffer
    participant TW as Timer Wheel
    participant D as Dispatcher
    T->>P: batch ReceiveMessage
    P->>P: NotBefore > 0?
    alt NotBefore set
        P->>TW: Insert(msg, delay)
        TW-->>R: expire → Enqueue
    else Immediate
        P->>R: Enqueue(msg)
    end
    D->>R: DequeueBatch
```

## Retry → DLQ Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Received
    Received --> Processing: Handler called
    Processing --> Acked: Result Ack
    Processing --> Retrying: Result Retry
    Processing --> DelayedRetry: Result RetryWithDelay
    Processing --> DeadLettered: Result DLQ
    Retrying --> RingBuf: re-enqueue
    DelayedRetry --> Transport: Nack (vis timeout)
    Transport --> Received: re-deliver
    RingBuf --> Received: dispatcher picks up
    Retrying --> DeadLettered: max attempts
    DelayedRetry --> DeadLettered: max attempts
    Acked --> [*]: msg deleted
    DeadLettered --> [*]: SendToDLQ
```

## Graceful Shutdown

```mermaid
sequenceDiagram
    participant OS as SIGTERM
    participant PL as Pipeline
    participant PO as Poller
    participant RB as Ring Buffer
    participant W as Workers
    OS->>PL: signal received
    PL->>PO: cancel context
    PO->>PO: stop ReceiveMessage
    PL->>RB: flush timer wheel
    RB->>W: drain remaining msgs
    W->>W: finish inflight handlers
    W->>Transport: Ack completed
    PL->>W: stop dispatching
    W-->>PL: all workers exited
    PL-->>OS: graceful exit
```
