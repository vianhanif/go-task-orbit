# OpenTelemetry Observability

Demonstrates wiring OpenTelemetry tracing and metrics via `ringq.Hooks`.

**Showcases:**
- All 6 lifecycle hooks: `OnReceive`, `OnDispatch`, `OnComplete`, `OnError`, `OnRetry`, `OnDuplicate`
- Counter-based metrics (messages sent, messages failed)
- Structured logging per hook
- Zero OTel SDK dependency — hooks are plain Go functions

**Run:**
```bash
go run ./examples/otel/

# Output:
# [OTEL] batch received: 2 messages
# [OTEL] dispatching to handler: email.send
# [INFO] sending email to user@example.com: Welcome!
# [OTEL] handler completed: email.send (took 12µs) | total_sent=1
# [OTEL] dispatching to handler: email.send
# [INFO] sending email to admin@example.com: New order #123
# [OTEL] handler completed: email.send (took 6µs) | total_sent=2
```
