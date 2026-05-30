package idempotency

import (
	"context"
	"errors"
	"time"
)

var ErrKeyNotFound = errors.New("idempotency: key not found")

type RedisCmdable interface {
	Get(ctx context.Context, key string) (string, error)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	Del(ctx context.Context, keys ...string) (int64, error)
	Close() error
}

type RedisStore struct {
	client RedisCmdable
	prefix string
}

func NewRedisStore(client RedisCmdable, prefix string) *RedisStore {
	if prefix == "" {
		prefix = "idem:"
	}
	return &RedisStore{
		client: client,
		prefix: prefix,
	}
}

func (s *RedisStore) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.Get(ctx, s.prefix+key)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, ErrKeyNotFound) {
		return false, nil
	}
	return false, err
}

func (s *RedisStore) Mark(ctx context.Context, key string, ttl time.Duration) error {
	_, err := s.client.SetNX(ctx, s.prefix+key, "1", ttl)
	return err
}

func (s *RedisStore) Close() error {
	if s.client != nil {
		return s.client.Close()
	}
	return nil
}
