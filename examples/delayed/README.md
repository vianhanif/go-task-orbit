# ETA Delayed Tasks

Demonstrates scheduled/delayed message processing using `Message.NotBefore`.

**Showcases:**
- `NotBefore` field for delayed execution
- Timer wheel integration (internal, 1s tick precision)
- Mix of immediate and delayed messages in the same pipeline
- Works identically across all transports

**Run:**
```bash
go run ./examples/delayed/

# Output:
# Published immediate message
# [15:30:01] Sending reminder to user-001: Welcome!
# Published delayed message (NotBefore=5s)
# ... 5 seconds later ...
# [15:30:06] Sending reminder to user-001: Reminder: your trial expires in 3 days
```
