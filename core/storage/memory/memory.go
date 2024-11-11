package memory

import (
	"context"
	"time"

	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/types"
)

// MemoryStorage provides in-memory storage implementation
type MemoryStorage[K types.GroupKey, T any] struct {
	windows map[time.Time]storage.WindowState[K, T]
}

func NewMemoryStorage[K types.GroupKey, T any]() *MemoryStorage[K, T] {
	return &MemoryStorage[K, T]{
		windows: make(map[time.Time]storage.WindowState[K, T]),
	}
}

func (m *MemoryStorage[K, T]) SaveWindow(ctx context.Context, state storage.WindowState[K, T]) error {
	m.windows[state.WindowStart] = state
	return nil
}

func (m *MemoryStorage[K, T]) LoadWindow(ctx context.Context, windowStart time.Time) (storage.WindowState[K, T], error) {
	state, ok := m.windows[windowStart]
	if !ok {
		return storage.WindowState[K, T]{}, nil
	}
	return state, nil
}

func (m *MemoryStorage[K, T]) DeleteWindow(ctx context.Context, windowStart time.Time) error {
	delete(m.windows, windowStart)
	return nil
}

func (m *MemoryStorage[K, T]) ListWindows(ctx context.Context, start, end time.Time) ([]time.Time, error) {
	var windows []time.Time
	for t := range m.windows {
		if (t.Equal(start) || t.After(start)) && t.Before(end) {
			windows = append(windows, t)
		}
	}
	return windows, nil
}
