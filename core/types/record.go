package types

import "time"

// Record represents a single data point with its metadata
type Record[T any] struct {
	Data      T
	Timestamp time.Time
	Metadata  map[string]interface{}
}
