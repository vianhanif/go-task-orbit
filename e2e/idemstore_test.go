//go:build e2e || e2e_gcp

package e2e

import (
	"context"
	"sync"
	"time"
)

type syncIdemStore struct {
	mu   sync.Mutex
	keys map[string]time.Time
}

func (s *syncIdemStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]time.Time)
	}
	exp, ok := s.keys[key]
	if !ok {
		return false, nil
	}
	if time.Now().After(exp) {
		delete(s.keys, key)
		return false, nil
	}
	return true, nil
}

func (s *syncIdemStore) Mark(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.keys == nil {
		s.keys = make(map[string]time.Time)
	}
	s.keys[key] = time.Now().Add(ttl)
	return nil
}

func (s *syncIdemStore) Close() error { return nil }
