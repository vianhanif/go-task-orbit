# Architecture

go-task-orbit is a cloud-native async execution runtime with ring-buffer scheduling and pluggable transports.

## Pipeline

```mermaid
flowchart LR
    Producer -->|Publish| Transport
    Transport -->|Receive| Poller
    Poller -->|Enqueue| Ring[Ring Buffer]
    Ring -->|Batch Dequeue| Dispatcher
    Dispatcher -->|Filter| Idem[Idempotency]
    Idem -->|Dispatch| Workers[Worker Pool]
    Workers -->|Ack| Transport
    Workers -->|Retry| Ring
    Workers -->|DLQ| Transport
```

## Receive Loop

```mermaid
sequenceDiagram
    participant SQS as Transport (SQS)
    participant Poller
    participant Ring
    participant Timer as Timer Wheel
    participant Dispatch

    SQS->>Poller: batch ReceiveMessage (up to 10)
    Poller->>Poller: NotBefore > 0?
    alt NotBefore > 0
        Poller->>Timer: Insert(msg, delay)
        Timer-->>Ring: expire → Enqueue
    else Immediate
        Poller->>Ring: Enqueue(msg)
    end
    Poller->>Poller: return nil (continue loop)
    Dispatch->>Ring: DequeueBatch(10)
```

## Retry → DLQ Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Received
    Received --> Processing: Handler called
    Processing --> Acked: Result{Ack}
    Processing --> Retrying: Result{Retry}
    Processing --> DelayedRetry: Result{RetryWithDelay}
    Processing --> DeadLettered: Result{DLQ}
    Retrying --> RingBuffer: re-enqueue
    DelayedRetry --> Transport: Nack (visibility timeout)
    Transport --> Received: re-deliver after delay
    RingBuffer --> Received: dispatcher picks up
    Retrying --> DeadLettered: max attempts exceeded
    DelayedRetry --> DeadLettered: max attempts exceeded
    Acked --> [*]: DeleteMessage / msg.Ack()
    DeadLettered --> [*]: SendToDLQ / Nack
```

## Graceful Shutdown

```mermaid
sequenceDiagram
    participant OS as SIGTERM
    participant Pipeline
    participant Poller
    participant Ring
    participant Workers

    OS->>Pipeline: signal received
    Pipeline->>Poller: cancel context
    Poller->>Poller: stop ReceiveMessage loop
    Pipeline->>Ring: flush pending timers
    Ring->>Workers: drain remaining messages
    Workers->>Workers: finish inflight handlers
    Workers->>Transport: Ack completed
    Pipeline->>Workers: stop dispatching
    Workers-->>Pipeline: all workers exited
    Pipeline-->>OS: graceful exit
```

## Ring Buffer

```mermaid
flowchart TB
    subgraph "Ring Buffer"
        direction LR
        H[Head Cursor] -->|write| S0[Slot 0]
        S0 --> S1[Slot 1]
        S1 --> S2[Slot 2]
        S2 --> SD[...]
        SD --> SN[Slot N-1]
        SN --> S0
    end

    Poller[Poller] -->|Enqueue| H
    Dispatch[Dispatcher] -->|DequeueBatch| T[Tail Cursor]
```
