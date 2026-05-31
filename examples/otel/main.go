package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

// This example demonstrates wiring OpenTelemetry via ringq.Hooks.
// In production, replace the hook bodies with actual OTel span/metric calls:
//
//   OnReceive: func(ctx context.Context, count int) {
//       _, span := tracer.Start(ctx, "sqs.receive", trace.WithAttributes(attribute.Int("count", count)))
//       defer span.End()
//   },
//   OnComplete: func(ctx context.Context, topic string, dur time.Duration) {
//       metric.Record(ctx, "handler.duration", metric.WithAttributes(attribute.String("topic", topic)), dur)
//   },

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

var (
	messagesSent   int64
	messagesFailed int64
)

func sendEmail(ctx context.Context, msg EmailPayload) ringq.Result {
	fmt.Printf("[INFO] sending email to %s: %s\n", msg.To, msg.Subject)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	transport := memory.New()

	pipeline := ringq.New().
		Transport(transport).
		Handle("email.send", ringq.Wrap(sendEmail)).
		WithHooks(ringq.Hooks{
			OnReceive: func(_ context.Context, count int) {
				fmt.Printf("[OTEL] batch received: %d messages\n", count)
			},
			OnDispatch: func(_ context.Context, topic string) {
				fmt.Printf("[OTEL] dispatching to handler: %s\n", topic)
			},
			OnComplete: func(_ context.Context, topic string, dur time.Duration) {
				messagesSent++
				fmt.Printf("[OTEL] handler completed: %s (took %v) | total_sent=%d\n", topic, dur, messagesSent)
			},
			OnError: func(_ context.Context, topic string, err error) {
				messagesFailed++
				fmt.Printf("[OTEL] handler error: %s | err=%v | total_failed=%d\n", topic, err, messagesFailed)
			},
			OnRetry: func(_ context.Context, topic string, msg ringq.Message, attempt int) {
				fmt.Printf("[OTEL] retry: %s (attempt %d) | msg_id=%s\n", topic, attempt, msg.ID)
			},
			OnDuplicate: func(_ context.Context, key string) {
				fmt.Printf("[OTEL] duplicate filtered: key=%s\n", key)
			},
		}).
		Concurrency(4).
		BufferSize(1024)

	go pipeline.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "email.send",
		Payload: []byte(`{"to":"user@example.com","subject":"Welcome!","body":"Thanks for signing up"}`),
	})
	transport.Publish(ctx, ringq.Message{
		ID:      "2",
		Topic:   "email.send",
		Payload: []byte(`{"to":"admin@example.com","subject":"New order #123","body":"Order details..."}`),
	})

	time.Sleep(300 * time.Millisecond)
	fmt.Println("Done. Press Ctrl+C to exit.")
	<-ctx.Done()
}
