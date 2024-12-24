package wal

import (
	"fmt"
	"io"
	"sync"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/recordio"
)

type WAL struct {
	mu sync.Mutex
	w  io.WriteCloser
}

func NewWAL(w io.WriteCloser) *WAL {
	return &WAL{w: w}
}

func (w *WAL) Write(rec partition.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := recordio.Write(w.w, rec); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.w.Close()
}
