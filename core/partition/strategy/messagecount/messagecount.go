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

func NewStrategy(maxMessages int) *Strategy {
	return &Strategy{
		MaxMessages: maxMessages,
		counter:     make(map[string]int),
	}
}

func (s *Strategy) ShouldRotate(first, _ partition.Record) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counter[first.GetPartitionKey()]++
	return s.counter[first.GetPartitionKey()] >= s.MaxMessages
}
