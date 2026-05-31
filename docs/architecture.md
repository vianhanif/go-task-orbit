# Architecture

go-task-orbit is a cloud-native async execution runtime with ring-buffer scheduling and pluggable transports.

## Pipeline

![Pipeline](diagram-pipeline.png)

## Receive Loop

![Receive Loop](diagram-receive-loop.png)

## Retry → DLQ Lifecycle

![Retry Lifecycle](diagram-retry-lifecycle.png)

## Graceful Shutdown

![Shutdown Sequence](diagram-shutdown.png)
