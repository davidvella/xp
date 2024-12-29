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

type Writer struct {
	writer         recordio.BinaryWriter
	currentOffset  atomic.Int64
	currentSegment atomic.Pointer[segment]
	maxRecords     int
	closed         atomic.Bool
	wc             io.WriteCloser
	mu             sync.Mutex
}

type segment struct {
	records *btree.BTreeG[partition.Record]
}

func NewWriter(wc io.WriteCloser, maxRecords int) (*Writer, error) {
	if maxRecords <= 0 {
		return nil, ErrInvalidMaxRecords
	}

	w := &Writer{
		writer:     recordio.NewBinaryWriter(wc),
		maxRecords: maxRecords,
		wc:         wc,
	}

	w.newSegment()

	return w, nil
}

func (w *Writer) newSegment() {
	seg := &segment{
		records: btree.NewG[partition.Record](2, func(a, b partition.Record) bool {
			return a.Less(b)
		}),
	}
	w.currentSegment.Store(seg)
}

func (w *Writer) Write(record partition.Record) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	seg := w.currentSegment.Load()
	seg.records.ReplaceOrInsert(record)

	if seg.records.Len() >= w.maxRecords {
		if err := w.flushSegment(seg.records); err != nil {
			return err
		}
		w.newSegment()
	}

	return nil
}

func (w *Writer) flushSegment(s *btree.BTreeG[partition.Record]) error {
	var totalSize = recordio.Int64Size

	s.Ascend(func(record partition.Record) bool {
		totalSize += recordio.Size(record)
		return true
	})

	if _, err := w.writer.WriteInt64(totalSize); err != nil {
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

	w.currentOffset.Add(totalSize)
	return nil
}

func (w *Writer) Close() error {
	if w.closed.Swap(true) {
		return ErrWALClosed
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	seg := w.currentSegment.Load()
	if seg.records.Len() > 0 {
		if err := w.flushSegment(seg.records); err != nil {
			return err
		}
	}

	return w.wc.Close()
}
