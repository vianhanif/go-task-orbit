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

// This example demonstrates ETA delayed task scheduling using Message.NotBefore.
// Messages with NotBefore > 0 are held in the timer wheel until the delay expires,
// then automatically inserted into the ring buffer for processing.

type ReminderPayload struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func sendReminder(ctx context.Context, msg ReminderPayload) ringq.Result {
	fmt.Printf("[%s] Sending reminder to user %s: %s\n", time.Now().Format("15:04:05"), msg.UserID, msg.Message)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	transport := memory.New()

	pipeline := ringq.New().
		Transport(transport).
		Handle("reminder.send", ringq.Wrap(sendReminder)).
		Concurrency(2).
		BufferSize(16)

	go pipeline.Run(ctx)
	time.Sleep(200 * time.Millisecond)

	// Immediate delivery — NotBefore = 0
	transport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "reminder.send",
		Payload: []byte(`{"user_id":"user-001","message":"Welcome!"}`),
	})
	fmt.Println("Published immediate message")

	// Delayed delivery — wait 5 seconds before processing
	transport.Publish(ctx, ringq.Message{
		ID:        "2",
		Topic:     "reminder.send",
		Payload:   []byte(`{"user_id":"user-001","message":"Reminder: your trial expires in 3 days"}`),
		NotBefore: 5 * time.Second,
	})
	fmt.Println("Published delayed message (NotBefore=5s)")

	time.Sleep(8 * time.Second)
	cancel()
	fmt.Println("Done.")
}
