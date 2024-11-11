package types

import "time"

// Window represents a time window with its data
type Window[T any] struct {
	StartTime time.Time
	EndTime   time.Time
	Data      []Record[T]
}
