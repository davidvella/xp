package timewindow

import (
	"time"

	"github.com/davidvella/xp/core/partition"
)

type Strategy struct {
	windowSize time.Duration
}

func NewStrategy(windowSize time.Duration) *Strategy {
	return &Strategy{
		windowSize: windowSize,
	}
}

func (s *Strategy) ShouldRotate(information partition.Information, watermark time.Time) bool {
	return watermark.Sub(information.FirstWatermark) > s.windowSize
}
