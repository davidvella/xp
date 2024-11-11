package types

import "time"

// GroupKey represents a composite key for grouping
type GroupKey interface {
	comparable
}

// GroupedData represents grouped records within a window
type GroupedData[K GroupKey, T any] struct {
	Key   K
	Items []Record[T]
}

// Watermark represents the progress of event time
type Watermark struct {
	Timestamp time.Time
}
