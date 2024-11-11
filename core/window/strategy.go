package window

import (
	"time"

	"github.com/davidvella/xp/core/types"
)

// WindowStrategy defines how records are assigned to windows
type WindowStrategy[T any] interface {
	// AssignWindows assigns a record to one or more windows
	AssignWindows(record types.Record[T]) []time.Time
	// IsWindowComplete determines if a window is ready to be processed
	IsWindowComplete(window time.Time, watermark time.Time) bool
}

// FixedWindowStrategy implements fixed-size windows
type FixedWindowStrategy[T any] struct {
	size     time.Duration
	maxDelay time.Duration
}

func NewFixedWindowStrategy[T any](size, maxDelay time.Duration) *FixedWindowStrategy[T] {
	return &FixedWindowStrategy[T]{
		size:     size,
		maxDelay: maxDelay,
	}
}

func (s *FixedWindowStrategy[T]) AssignWindows(record types.Record[T]) []time.Time {
	windowStart := record.Timestamp.Truncate(s.size)
	return []time.Time{windowStart}
}

func (s *FixedWindowStrategy[T]) IsWindowComplete(window, watermark time.Time) bool {
	return watermark.After(window.Add(s.size).Add(s.maxDelay))
}

// SlidingWindowStrategy implements sliding windows
type SlidingWindowStrategy[T any] struct {
	size     time.Duration
	slide    time.Duration
	maxDelay time.Duration
}

func NewSlidingWindowStrategy[T any](size, slide, maxDelay time.Duration) *SlidingWindowStrategy[T] {
	return &SlidingWindowStrategy[T]{
		size:     size,
		slide:    slide,
		maxDelay: maxDelay,
	}
}

func (s *SlidingWindowStrategy[T]) AssignWindows(record types.Record[T]) []time.Time {
	var windows []time.Time
	timestamp := record.Timestamp
	currentStart := timestamp.Truncate(s.slide)

	for currentStart.Add(s.size).After(timestamp) {
		windows = append(windows, currentStart)
		currentStart = currentStart.Add(-s.slide)
	}

	return windows
}

func (s *SlidingWindowStrategy[T]) IsWindowComplete(window, watermark time.Time) bool {
	return watermark.After(window.Add(s.size).Add(s.maxDelay))
}
