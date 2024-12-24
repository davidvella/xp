package writer

import (
	"context"
	"fmt"
	"sync"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/storage"
	"github.com/davidvella/xp/core/wal"
)

type Writer struct {
	storage     storage.Storage
	strategy    partition.Strategy
	mu          sync.RWMutex
	activeFiles map[string]*activeFile
}

type activeFile struct {
	writer      *wal.WAL
	firstRecord partition.Record
}

func New(storage storage.Storage, strategy partition.Strategy) *Writer {
	return &Writer{
		storage:     storage,
		strategy:    strategy,
		activeFiles: make(map[string]*activeFile),
	}
}

func (w *Writer) Write(ctx context.Context, record partition.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	partitionKey := record.PartitionKey
	active, exists := w.activeFiles[partitionKey]

	shouldRotate := exists && w.strategy.ShouldRotate(active.firstRecord, record)
	if !exists || shouldRotate {
		if shouldRotate {
			if err := w.rotate(ctx, partitionKey); err != nil {
				return fmt.Errorf("failed to rotate: %w", err)
			}
		}

		filename := fmt.Sprintf("%s_%d.dat", partitionKey, record.Timestamp.Unix())
		writer, err := w.storage.Create(ctx, filename)
		if err != nil {
			return fmt.Errorf("failed to create file: %w", err)
		}

		active = &activeFile{
			writer:      wal.NewWAL(writer),
			firstRecord: record,
		}
		w.activeFiles[partitionKey] = active
	}

	if err := active.writer.Write(record); err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}

	return nil
}

func (w *Writer) rotate(ctx context.Context, path string) error {
	active := w.activeFiles[path]
	if active == nil {
		return nil
	}

	if err := active.writer.Close(); err != nil {
		return err
	}

	if err := w.storage.Publish(ctx, path); err != nil {
		return err
	}

	delete(w.activeFiles, path)
	return nil
}

func (w *Writer) Close(ctx context.Context) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for path := range w.activeFiles {
		if err := w.rotate(ctx, path); err != nil {
			return fmt.Errorf("failed to rotate during close: %v", err)
		}
	}
	return nil
}

func (w *Writer) Recover(ctx context.Context) error {
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
