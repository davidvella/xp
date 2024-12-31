package processor

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/priority"
	"github.com/davidvella/xp/wal"
)

// Storage defines the interface for the underlying storage system.
type Storage interface {
	// Create a new file/object for writing
	Create(ctx context.Context, path string) (io.WriteCloser, error)
	// Publish a file/object from pending to publishing
	Publish(ctx context.Context, path string) error
	// List files/objects from pending
	List(ctx context.Context) ([]string, error)
}

type Processor struct {
	storage     Storage
	strategy    partition.Strategy
	mu          sync.RWMutex
	activeFiles *priority.Queue[string, activeWriter]
}

type activeWriter struct {
	mu            *sync.RWMutex
	writer        *wal.Writer
	information   partition.Information
	name          string
	lastWatermark time.Time
}

func newActiveWriter(writer io.WriteCloser, record partition.Record, name string) (activeWriter, error) {
	w, err := wal.NewWriter(writer, 1000)
	if err != nil {
		return activeWriter{}, err
	}
	return activeWriter{
		writer: w,
		information: partition.Information{
			PartitionKey:   record.GetPartitionKey(),
			RecordCount:    0,
			FirstWatermark: record.GetWatermark(),
		},
		lastWatermark: record.GetWatermark(),
		mu:            &sync.RWMutex{},
		name:          name,
	}, nil
}

func (w *activeWriter) Write(rec partition.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Write(rec); err != nil {
		return err
	}

	w.lastWatermark = rec.GetWatermark()
	w.information.RecordCount++

	return nil
}

func (w *activeWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.Close()
}

func New(storage Storage, strategy partition.Strategy) *Processor {
	return &Processor{
		storage:     storage,
		strategy:    strategy,
		activeFiles: priority.NewQueue[string, activeWriter](orderByWatermark),
	}
}

func (w *Processor) Handle(ctx context.Context, record partition.Record) error {
	w.mu.RLock()

	partitionKey := record.GetPartitionKey()
	active, exists := w.activeFiles.Get(partitionKey)
	shouldRotate := !exists || w.strategy.ShouldRotate(active.information, record.GetWatermark())

	w.mu.RUnlock()

	if shouldRotate {
		var err error
		if active, err = w.getActiveWriter(ctx, record, partitionKey); err != nil {
			return err
		}
	}

	if err := active.Write(record); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	w.activeFiles.Set(partitionKey, active)

	if err := w.cleanup(ctx, record.GetWatermark()); err != nil {
		return err
	}

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
			return fmt.Errorf("failed to rotate during close: %w", err)
		}
	}

	return nil
}

func (w *Processor) Recover(ctx context.Context) error {
	files, err := w.storage.List(ctx)
	if err != nil {
		return fmt.Errorf("failed to list pending files: %w", err)
	}

	for _, file := range files {
		if err := w.storage.Publish(ctx, file); err != nil {
			return fmt.Errorf("failed to publish recovered file: %w", err)
		}
	}

	return nil
}

func (w *Processor) getActiveWriter(ctx context.Context, record partition.Record, partitionKey string) (activeWriter, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.rotate(ctx, partitionKey); err != nil {
		return activeWriter{}, fmt.Errorf("failed to rotate: %w", err)
	}

	writerName := Serialize(WalKey{
		PartitionKey: partitionKey,
		Watermark:    record.GetWatermark(),
	})
	writer, err := w.storage.Create(ctx, writerName)
	if err != nil {
		return activeWriter{}, fmt.Errorf("failed to create writer: %w", err)
	}

	active, err := newActiveWriter(writer, record, writerName)
	if err != nil {
		return activeWriter{}, fmt.Errorf("failed to create writer: %w", err)
	}

	w.activeFiles.Set(partitionKey, active)

	return active, nil
}

func (w *Processor) rotate(ctx context.Context, partitionKey string) error {
	active, found := w.activeFiles.Get(partitionKey)
	if !found {
		return nil
	}

	if err := active.Close(); err != nil {
		return err
	}

	if err := w.storage.Publish(ctx, active.name); err != nil {
		return err
	}

	w.activeFiles.Remove(partitionKey)
	return nil
}

func (w *Processor) cleanup(ctx context.Context, watermark time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for {
		k, v, ok := w.activeFiles.Peek()
		if !ok {
			break
		}
		if !w.strategy.ShouldRotate(v.information, watermark) {
			break
		}
		if err := w.rotate(ctx, k); err != nil {
			return fmt.Errorf("failed to rotate during close: %w", err)
		}
		w.activeFiles.Remove(k)
	}

	return nil
}

func orderByWatermark(a, b activeWriter) bool {
	return a.lastWatermark.Before(b.lastWatermark)
}
