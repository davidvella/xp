package messagecount

import (
	"sync"

	"github.com/davidvella/xp/core/partition"
)

type Strategy struct {
	MaxMessages int
	mu          sync.RWMutex
	counter     map[string]int
}

func New(maxMessages int) *Strategy {
	return &Strategy{
		MaxMessages: maxMessages,
		counter:     make(map[string]int),
	}
}

func (s *Strategy) ShouldRotate(first, incoming partition.Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counter[first.PartitionKey]++
	return s.counter[first.PartitionKey] >= s.MaxMessages
}
