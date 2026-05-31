# Idempotency

Demonstrates library-managed message deduplication.

**Showcases:**
- `IdempotencyConfig` with `MemoryStore` (dev/test)
- Idempotency key read from message attributes (`IdempotencyKey`)
- `OnDuplicate` hook for logging filtered duplicates
- First message processed, duplicate acked silently

**Run:**
```bash
go run ./examples/idempotent/

# Output:
# Processing order ORD-001 for $99.95
# Duplicate message filtered: key=ord-001-v1
# Processing order ORD-002 for $149.50
```

For multi-pod production, replace `MemoryStore` with `idempotency.NewRedisStore(redisClient, "idem:")`.
