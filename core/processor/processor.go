package processor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/wal"
)

type Processor struct {
	storage     storage.Storage
	strategy    partition.Strategy
	mu          sync.RWMutex
	activeFiles *PriorityQueue[string, *activeWriter]
}

type activeWriter struct {
	sync.RWMutex
	writer        *wal.WAL
	firstRecord   partition.Record
	lastWatermark time.Time
}

func (w *activeWriter) Write(rec partition.Record) error {
	w.Lock()
	defer w.Unlock()

	if err := w.writer.Write(rec); err != nil {
		return err
	}

	w.lastWatermark = rec.GetWatermark()

	return nil
}

func (w *activeWriter) Close() error {
	w.Lock()
	defer w.Unlock()

	return w.writer.Close()
}

func New(storage storage.Storage, strategy partition.Strategy) *Processor {
	return &Processor{
		storage:  storage,
		strategy: strategy,
		activeFiles: NewPriorityQueue[string, *activeWriter](func(a, b *activeWriter) bool {
			if a == nil {
				return true
			}
			if b == nil {
				return false
			}
			return a.lastWatermark.Before(b.lastWatermark)
		}),
	}
}

func (w *Processor) Handle(ctx context.Context, record partition.Record) error {
	w.mu.RLock()

	partitionKey := record.GetPartitionKey()
	active, exists := w.activeFiles.Get(partitionKey)
	shouldRotate := exists && w.strategy.ShouldRotate(active.firstRecord, record)

	w.mu.RUnlock()

	if !exists || shouldRotate {
		var err error
		if active, err = w.getActiveWriter(ctx, record, partitionKey, shouldRotate); err != nil {
			return err
		}
	}

	// Use the thread-safe Write method of activeWriter
	if err := active.Write(record); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	w.activeFiles.Set(partitionKey, active)

	return nil
}

func (w *Processor) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		k, _, ok := w.activeFiles.Pop()
		if !ok {
			break
		}
		if err := w.rotate(ctx, k); err != nil {
			return fmt.Errorf("failed to rotate during close: %v", err)
		}
	}

	return nil
}

func (w *Processor) Recover(ctx context.Context) error {
	files, err := w.storage.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list pending files: %v", err)
	}

	for _, file := range files {
		if err := w.storage.Publish(ctx, file); err != nil {
			return fmt.Errorf("failed to publish recovered file: %v", err)
		}
	}

	return nil
}

func (w *Processor) getActiveWriter(ctx context.Context, record partition.Record, partitionKey string, shouldRotate bool) (*activeWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if shouldRotate {
		if err := w.rotate(ctx, partitionKey); err != nil {
			return nil, fmt.Errorf("failed to rotate: %w", err)
		}
	}

	filename := fmt.Sprintf("%s_%d.dat", partitionKey, record.GetWatermark().Unix())
	writer, err := w.storage.Create(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	active := &activeWriter{
		writer:        wal.NewWAL(writer),
		firstRecord:   record,
		lastWatermark: record.GetWatermark(),
	}
	w.activeFiles.Set(partitionKey, active)

	return active, nil
}

func (w *Processor) rotate(ctx context.Context, path string) error {
	active, ok := w.activeFiles.Get(path)
	if !ok {
		return nil
	}

	if err := active.Close(); err != nil {
		return err
	}

	if err := w.storage.Publish(ctx, path); err != nil {
		return err
	}

	w.activeFiles.Remove(path)
	return nil
}
