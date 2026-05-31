# Architecture

go-task-orbit is a cloud-native async execution runtime with ring-buffer scheduling and pluggable transports.

## Pipeline

![Pipeline: Producer → Transport → Poller → Ring Buffer → Dispatcher → Idempotency → Worker Pool → Ack/Retry/DLQ](diagram-pipeline.svg)

## Receive Loop

![Receive Loop: Transport → Poller → Ring Buffer / Timer Wheel → Dispatcher](diagram-receive-loop.svg)

## Retry → DLQ Lifecycle

![Retry → DLQ Lifecycle: state machine from receipt to terminal](diagram-retry-lifecycle.svg)

## Graceful Shutdown

![Graceful Shutdown: SIGTERM → cancel poller → flush timer → drain ring → ack inflight → exit](diagram-shutdown.svg)
