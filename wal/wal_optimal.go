package wal

import (
	"errors"
	"io"
	"iter"
	"os"
	"sync"
	"sync/atomic"

	"github.com/davidvella/xp/loser"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

var (
	ErrInvalidMaxRecords = errors.New("maxRecords must be greater than 0")
	ErrWALClosed         = errors.New("WAL is closed")
)

type WAL struct {
	file          *os.File
	writer        recordio.BinaryWriter
	currentOffset atomic.Int64
	segments      []*segment
	segmentLock   sync.RWMutex
	writeChan     chan partition.Record
	quitChan      chan struct{}
	wg            sync.WaitGroup
	maxRecords    int
	closed        atomic.Bool
	filePath      string // Store filepath for reopening
}

type segment struct {
	records *BTree
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
		records: New(2),
	}
}

func NewWAL(filePath string, maxRecords int) (*WAL, error) {
	if maxRecords <= 0 {
		return nil, ErrInvalidMaxRecords
	}

	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	wal := &WAL{
		file:       file,
		writer:     recordio.NewBinaryWriter(file),
		segments:   []*segment{newSegment()},
		writeChan:  make(chan partition.Record, 1000), // Buffered channel
		quitChan:   make(chan struct{}),
		maxRecords: maxRecords,
		filePath:   filePath,
	}

	wal.currentOffset.Store(info.Size())

	wal.wg.Add(1)
	go wal.writeLoop()

	return wal, nil
}

func (w *WAL) Write(record partition.Record) error {
	if w.closed.Load() {
		return ErrWALClosed
	}

	select {
	case w.writeChan <- record:
		return nil
	case <-w.quitChan:
		return ErrWALClosed
	}
}

func (w *WAL) writeLoop() {
	defer w.wg.Done()

	for {
		select {
		case record := <-w.writeChan:
			if err := w.handleRecord(record); err != nil {
				// Log error here
				continue
			}
		case <-w.quitChan:
			return
		}
	}
}

func (w *WAL) handleRecord(record partition.Record) error {
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

		// Set the length after successful flush
		currentSegment.mu.Lock()
		currentSegment.length = w.currentOffset.Load() - currentOffset
		currentSegment.mu.Unlock()
	} else {
		currentSegment.mu.Unlock()
	}

	return nil
}

func (w *WAL) flushSegment(s *BTree) error {
	var totalSize int64

	// Pre-calculate size
	s.Ascend(func(record partition.Record) bool {
		totalSize += recordio.Size(record)
		return true
	})

	lenSize, err := w.writer.WriteInt64(totalSize)
	if err != nil {
		return err
	}
	totalSize += lenSize

	// Write records and handle errors
	var writeErr error
	s.Ascend(func(record partition.Record) bool {
		if _, err := recordio.Write(w.file, record); err != nil {
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

func (w *WAL) ReadAll() iter.Seq[partition.Record] {
	// Create a snapshot of segments to prevent modification during reading
	w.segmentLock.RLock()
	segmentsCopy := make([]*segment, len(w.segments))
	copy(segmentsCopy, w.segments)
	w.segmentLock.RUnlock()

	var sequences []loser.Sequence[partition.Record]

	for _, seg := range segmentsCopy {
		seg.mu.RLock()
		if seg.flushed {
			reader := &segmentReader{
				wal:    w,
				offset: seg.offset,
				length: seg.length,
			}
			sequences = append(sequences, reader)
		} else {
			records := seg.records
			sequences = append(sequences, &memorySegmentReader{records: records})
		}
		seg.mu.RUnlock()
	}

	tree := loser.New(sequences, nil)
	return tree.All()
}

func (w *WAL) Close() error {
	if w.closed.Swap(true) {
		return ErrWALClosed
	}

	close(w.quitChan)
	w.wg.Wait()

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

	return w.file.Close()
}

type segmentReader struct {
	wal    *WAL
	offset int64
	length int64
	mu     sync.Mutex
}

func (sr *segmentReader) All() iter.Seq[partition.Record] {
	sr.mu.Lock()
	defer sr.mu.Unlock()

	// If WAL is closed, open a new file handle for reading
	var reader io.Reader
	if sr.wal.closed.Load() {
		file, err := os.OpenFile(sr.wal.filePath, os.O_RDONLY, 0644)
		if err != nil {
			// Return empty sequence on error
			return func(yield func(partition.Record) bool) {}
		}
		defer file.Close()
		reader = io.NewSectionReader(file, sr.offset, sr.length)
	} else {
		reader = io.NewSectionReader(sr.wal.file, sr.offset, sr.length)
	}

	return recordio.Seq(reader)
}

type memorySegmentReader struct {
	records *BTree
}

func (mr *memorySegmentReader) All() iter.Seq[partition.Record] {
	return func(yield func(partition.Record) bool) {
		mr.records.Ascend(func(record partition.Record) bool {
			return !yield(record)
		})
	}
}
