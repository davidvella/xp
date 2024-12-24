package processor

import (
	"context"
	"fmt"
	"sync"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/wal"
)

type Processor struct {
	storage     storage.Storage
	strategy    partition.Strategy
	mu          sync.RWMutex
	activeFiles map[string]*activeFile
}

type activeFile struct {
	writer      *wal.WAL
	firstRecord partition.Record
	mu          sync.RWMutex
}

func (w *activeFile) Write(rec partition.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.Write(rec)
}

func (w *activeFile) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.writer.Close()
}

func New(storage storage.Storage, strategy partition.Strategy) *Processor {
	return &Processor{
		storage:     storage,
		strategy:    strategy,
		activeFiles: make(map[string]*activeFile),
	}
}

// Handle - The assumption that for each partition key messages are processed in order.
func (w *Processor) Handle(ctx context.Context, record partition.Record) error {
	w.mu.RLock()

	partitionKey := record.GetPartitionKey()
	active, exists := w.activeFiles[partitionKey]
	shouldRotate := exists && w.strategy.ShouldRotate(active.firstRecord, record)

	w.mu.RUnlock()

	if !exists || shouldRotate {
		var err error
		if active, err = w.getActiveFile(ctx, record, partitionKey, shouldRotate); err != nil {
			return err
		}
	}

	// Use the thread-safe Write method of activeFile
	if err := active.Write(record); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	return nil
}

func (w *Processor) getActiveFile(ctx context.Context, record partition.Record, partitionKey string, shouldRotate bool) (*activeFile, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if shouldRotate {
		if err := w.rotate(ctx, partitionKey); err != nil {
			return nil, fmt.Errorf("failed to rotate: %w", err)
		}
	}

	filename := fmt.Sprintf("%s_%d.dat", partitionKey, record.GetTimestamp().Unix())
	writer, err := w.storage.Create(ctx, filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create file: %w", err)
	}

	active := &activeFile{
		writer:      wal.NewWAL(writer),
		firstRecord: record,
	}
	w.activeFiles[partitionKey] = active

	return active, nil
}

func (w *Processor) rotate(ctx context.Context, path string) error {
	active := w.activeFiles[path]
	if active == nil {
		return nil
	}

	if err := active.Close(); err != nil {
		return err
	}

	if err := w.storage.Publish(ctx, path); err != nil {
		return err
	}

	delete(w.activeFiles, path)
	return nil
}

func (w *Processor) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for path := range w.activeFiles {
		if err := w.rotate(ctx, path); err != nil {
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
