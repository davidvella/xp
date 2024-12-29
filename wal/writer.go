package wal

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
	"github.com/google/btree"
)

var (
	ErrInvalidMaxRecords = errors.New("maxRecords must be greater than 0")
	ErrWALClosed         = errors.New("WAL is closed")
)

type segment struct {
	records *btree.BTreeG[partition.Record]
	flushed bool
	offset  int64
	length  int64
	mu      sync.RWMutex
}

func (s *segment) Len() int {
	return s.records.Len()
}

func newSegment() *segment {
	return &segment{
		records: btree.NewG[partition.Record](2, func(a, b partition.Record) bool {
			return a.Less(b)
		}),
	}
}

type Writer struct {
	writer        recordio.BinaryWriter
	currentOffset atomic.Int64
	segments      []*segment
	segmentLock   sync.RWMutex
	maxRecords    int
	closed        atomic.Bool
	wc            io.WriteCloser
}

func NewWriter(wc io.WriteCloser, maxRecords int) (*Writer, error) {
	if maxRecords <= 0 {
		return nil, ErrInvalidMaxRecords
	}

	w := &Writer{
		writer:     recordio.NewBinaryWriter(wc),
		segments:   []*segment{newSegment()},
		maxRecords: maxRecords,
		wc:         wc,
	}

	return w, nil
}

func (w *Writer) Write(record partition.Record) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.segmentLock.RLock()
	currentSegment := w.segments[len(w.segments)-1]
	w.segmentLock.RUnlock()

	currentSegment.mu.Lock()
	if currentSegment.flushed {
		currentSegment.mu.Unlock()

		w.segmentLock.Lock()
		if w.segments[len(w.segments)-1].flushed {
			w.segments = append(w.segments, newSegment())
			currentSegment = w.segments[len(w.segments)-1]
		} else {
			currentSegment = w.segments[len(w.segments)-1]
		}
		w.segmentLock.Unlock()

		currentSegment.mu.Lock()
	}

	currentSegment.records.ReplaceOrInsert(record)

	if currentSegment.Len() >= w.maxRecords {
		recordsToFlush := currentSegment.records
		currentSegment.flushed = true
		currentOffset := w.currentOffset.Load()
		currentSegment.offset = currentOffset
		currentSegment.mu.Unlock()

		if err := w.flushSegment(recordsToFlush); err != nil {
			return err
		}

		currentSegment.mu.Lock()
		currentSegment.length = w.currentOffset.Load() - currentOffset
		currentSegment.records = nil
		currentSegment.mu.Unlock()
	} else {
		currentSegment.mu.Unlock()
	}

	return nil
}

func (w *Writer) flushSegment(s *btree.BTreeG[partition.Record]) error {
	var totalSize = recordio.Int64Size

	s.Ascend(func(record partition.Record) bool {
		totalSize += recordio.Size(record)
		return true
	})

	_, err := w.writer.WriteInt64(totalSize)
	if err != nil {
		return err
	}

	var writeErr error
	s.Ascend(func(record partition.Record) bool {
		if _, err := recordio.Write(w.wc, record); err != nil {
			writeErr = err
			return false
		}
		return true
	})

	if writeErr != nil {
		return writeErr
	}

	if closer, ok := w.wc.(interface{ Sync() error }); ok {
		if err := closer.Sync(); err != nil {
			return err
		}
	}

	w.currentOffset.Add(totalSize)
	return nil
}

func (w *Writer) Close() error {
	if w.closed.Swap(true) {
		return ErrWALClosed
	}

	w.segmentLock.Lock()
	defer w.segmentLock.Unlock()

	lastSegment := w.segments[len(w.segments)-1]
	lastSegment.mu.Lock()
	if !lastSegment.flushed && lastSegment.Len() > 0 {
		records := lastSegment.records
		offset := w.currentOffset.Load()
		lastSegment.offset = offset
		lastSegment.flushed = true
		lastSegment.mu.Unlock()

		if err := w.flushSegment(records); err != nil {
			return err
		}

		lastSegment.mu.Lock()
		lastSegment.length = w.currentOffset.Load() - offset
		lastSegment.mu.Unlock()
	} else {
		lastSegment.mu.Unlock()
	}

	return w.wc.Close()
}
