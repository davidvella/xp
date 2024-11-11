package storage

import (
	"context"
	"time"

	"github.com/davidvella/xp/core/types"
)

// WindowState represents the state of a window
type WindowState[K types.GroupKey, T any] struct {
	WindowStart time.Time
	WindowEnd   time.Time
	Groups      map[K][]types.Record[T]
}

// Storage defines the interface for persistent storage
type Storage[K types.GroupKey, T any] interface {
	// SaveWindow persists a window state
	SaveWindow(ctx context.Context, state WindowState[K, T]) error

	// LoadWindow retrieves a window state
	LoadWindow(ctx context.Context, windowStart time.Time) (WindowState[K, T], error)

	// DeleteWindow removes a window state
	DeleteWindow(ctx context.Context, windowStart time.Time) error

	// ListWindows returns all window start times within a range
	ListWindows(ctx context.Context, start, end time.Time) ([]time.Time, error)
}
