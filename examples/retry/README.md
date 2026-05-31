# Exponential Backoff Retry

Demonstrates the retry engine with exponential backoff and DLQ fallback.

**Showcases:**
- `HandleWithRetry` with custom max retries and base delay
- Exponential backoff: 1s → 2s → 4s → 8s... capped at 5 minutes
- `OnRetry` hook tracks attempt count
- Default max retries: 10, then routed to DLQ

**Run:**
```bash
go run ./examples/retry/

# Output:
# Attempt 1: payment PAY-001 failed — will retry
# [RETRY] topic=payment.process attempt=1 (delay grows: 1s → 2s → 4s... cap 5min)
# Attempt 2: payment PAY-001 failed — will retry
# [RETRY] topic=payment.process attempt=2
# Attempt 3: payment PAY-001 failed — will retry
# Attempt 4: payment PAY-001 processed successfully
# [DONE] topic=payment.process completed
```
