package wal

import (
	"fmt"
	"io"
	"sync"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

type Writer struct {
	mu sync.Mutex
	w  io.WriteCloser
}

func NewWriter(w io.WriteCloser) *Writer {
	return &Writer{w: w}
}

func (w *Writer) Write(rec partition.Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := recordio.Write(w.w, rec); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	return nil
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	return w.w.Close()
}
