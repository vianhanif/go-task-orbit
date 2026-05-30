package idempotency

import (
	"context"
	"sync"
	"time"
)

type memoryEntry struct {
	expiresAt time.Time
}

type MemoryStore struct {
	mu     sync.RWMutex
	keys   map[string]memoryEntry
	stopCh chan struct{}
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		keys:   make(map[string]memoryEntry),
		stopCh: make(chan struct{}),
	}
	go s.evictLoop()
	return s
}

func (s *MemoryStore) Exists(_ context.Context, key string) (bool, error) {
	s.mu.RLock()
	entry, ok := s.keys[key]
	s.mu.RUnlock()

	if !ok {
		return false, nil
	}

	if time.Now().After(entry.expiresAt) {
		s.mu.Lock()
		delete(s.keys, key)
		s.mu.Unlock()
		return false, nil
	}

	return true, nil
}

func (s *MemoryStore) Mark(_ context.Context, key string, ttl time.Duration) error {
	s.mu.Lock()
	s.keys[key] = memoryEntry{
		expiresAt: time.Now().Add(ttl),
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Close() error {
	close(s.stopCh)
	s.mu.Lock()
	s.keys = nil
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) evictLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.evict()
		case <-s.stopCh:
			return
		}
	}
}

func (s *MemoryStore) evict() {
	now := time.Now()
	s.mu.Lock()
	for k, entry := range s.keys {
		if now.After(entry.expiresAt) {
			delete(s.keys, k)
		}
	}
	s.mu.Unlock()
}
