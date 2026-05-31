package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/redis"
)

type Notification struct {
	UserID  string `json:"user_id"`
	Message string `json:"message"`
}

func sendNotification(ctx context.Context, msg Notification) ringq.Result {
	fmt.Printf("Sending notification to user %s: %s\n", msg.UserID, msg.Message)
	return ringq.Result{Action: ringq.Ack}
}

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	redisTransport := redis.New(redis.Config{
		Addr:     "localhost:6379",
		Channels: []string{"notifications"},
	})

	pipeline := ringq.New().
		Transport(redisTransport).
		Handle("notification.send", ringq.Wrap(sendNotification)).
		Concurrency(4).
		BufferSize(1024)

	go pipeline.Run(ctx)

	time.Sleep(200 * time.Millisecond)

	redisTransport.Publish(ctx, ringq.Message{
		ID:      "1",
		Topic:   "notification.send",
		Payload: []byte(`{"user_id":"user-001","message":"Your order has shipped!"}`),
	})
	redisTransport.Publish(ctx, ringq.Message{
		ID:      "2",
		Topic:   "notification.send",
		Payload: []byte(`{"user_id":"user-002","message":"Payment confirmed"}`),
	})

	time.Sleep(500 * time.Millisecond)
	fmt.Println("Done. Press Ctrl+C to exit.")
	<-ctx.Done()
}
