# SQS Transport

Production-ready pipeline with Amazon SQS transport.

**Showcases:**
- SQS transport with batch receive, batch ack, DLQ routing
- `HandleWithRetry` for different retry policies per topic
- `BaseEndpoint` for local emulator testing (Floci)
- Environment variable configuration for queue URLs

**Run:**
```bash
# Local (Floci emulator on port 4566)
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_SESSION_TOKEN=test
task floci-start
go run ./examples/sqs/

# Production
export SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789/orders-main
export SQS_DLQ_URL=https://sqs.us-east-1.amazonaws.com/123456789/orders-dlq
go run ./examples/sqs/
```
