package timewindow

import (
	"time"

	"github.com/davidvella/xp/core/partition"
)

type Strategy struct {
	windowSize time.Duration
}

func New(windowSize time.Duration) *Strategy {
	return &Strategy{
		windowSize: windowSize,
	}
}

func (s *Strategy) ShouldRotate(first, incoming partition.Record) bool {
	currentWindow := first.Timestamp.Unix() / int64(s.windowSize.Seconds())
	incomingWindow := incoming.Timestamp.Unix() / int64(s.windowSize.Seconds())
	return currentWindow != incomingWindow
}
