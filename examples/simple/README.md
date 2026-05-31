# Simple Worker

Basic go-task-orbit pipeline with InMemory transport.

**Showcases:**
- `ringq.New()` builder pattern
- `ringq.Wrap()` for type-safe generic handlers
- Topic-based routing (`Handle`)
- Graceful shutdown via `signal.NotifyContext`

**Run:**
```bash
go run ./examples/simple/
```
