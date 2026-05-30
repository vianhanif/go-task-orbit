package ringq

import "time"

type Config struct {
	Concurrency int
	BufferSize  int
}

type IdempotencyConfig struct {
	Store        IdemStore
	AttributeKey string
	TTL          time.Duration
}
