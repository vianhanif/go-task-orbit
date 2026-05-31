# Redis Pub/Sub Transport

Fire-and-forget broadcast messaging with Redis Pub/Sub transport.

**Showcases:**
- Redis PUBLISH/SUBSCRIBE transport
- JSON wire format for payload + attributes over Redis channels
- Topic routing via `X-Topic` message attribute
- Graceful shutdown

**Limitations:** No acknowledgment, no retry, no DLQ. For durable Redis processing, use Redis Streams transport.

**Run:**
```bash
# Requires Redis running on localhost:6379
docker run -d --name redis -p 6379:6379 redis:7-alpine
go run ./examples/redis/
docker rm -f redis
```
