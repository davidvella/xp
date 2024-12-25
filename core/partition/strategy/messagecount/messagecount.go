package messagecount

import (
	"time"

	"github.com/davidvella/xp/core/partition"
)

type Strategy struct {
	MaxMessages int
}

func NewStrategy(maxMessages int) *Strategy {
	return &Strategy{
		MaxMessages: maxMessages,
	}
}

func (s *Strategy) ShouldRotate(information partition.Information, _ time.Time) bool {
	return information.RecordCount >= s.MaxMessages
}
