package bench

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vianhanif/go-task-orbit/ring"
	"github.com/vianhanif/go-task-orbit/ringq"
	"github.com/vianhanif/go-task-orbit/transport/memory"
)

func BenchmarkRingBufferEnqueueDequeue(b *testing.B) {
	buf := ring.New(4096, ring.DropOldest)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Enqueue(i)
		buf.Dequeue()
	}
}

func BenchmarkRingBufferBatch(b *testing.B) {
	buf := ring.New(4096, ring.DropOldest)
	items := make([]interface{}, 10)
	for i := 0; i < 10; i++ {
		items[i] = i
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.EnqueueBatch(items)
		buf.DequeueBatch(10)
	}
}

func BenchmarkPipelineThroughput(b *testing.B) {
	tp := memory.New()
	var counter int32

	p := ringq.New().
		Transport(tp).
		Handle("test", func(_ context.Context, _ []byte) ringq.Result {
			atomic.AddInt32(&counter, 1)
			return ringq.Result{Action: ringq.Ack}
		}).
		BufferSize(4096).
		Concurrency(64)

	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)

	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tp.Publish(ctx, ringq.Message{
			ID:      "test-id",
			Topic:   "test",
			Payload: []byte("hello"),
		})
	}

	time.Sleep(200 * time.Millisecond)
	cancel()
}

func BenchmarkGoChannel(b *testing.B) {
	ch := make(chan int, 4096)
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	close(done)
}

func BenchmarkGoChannelBatch(b *testing.B) {
	ch := make(chan int, 4096)
	done := make(chan struct{})
	items := make([]int, 10)
	for i := 0; i < 10; i++ {
		items[i] = i
	}

	go func() {
		for {
			select {
			case <-ch:
			case <-done:
				return
			}
		}
	}()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, v := range items {
			ch <- v
		}
	}
	close(done)
}

func BenchmarkGoroutinePerMessage(b *testing.B) {
	var wg sync.WaitGroup

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(v int) {
			_ = v
			wg.Done()
		}(i)
	}
	wg.Wait()
}
