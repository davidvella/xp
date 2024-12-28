// Package sstable - Sorted String Table (SSTable) is one of the most popular
// outputs for storing, processing, and exchanging datasets. As the name itself
// implies, an SSTable is a simple abstraction to efficiently store large
// numbers of key-value pairs while optimizing for high throughput, sequential
// read/write workloads.
//
// A "Sorted String Table" then is exactly what it sounds like, it is a file
// which contains a set of arbitrary, sorted key-value pairs inside. Duplicate
// keys are fine, there is no need for "padding" for keys or values, and keys
// and values are arbitrary blobs. Read in the entire file sequentially and you
// have a sorted sparseIndex. Optionally, if the file is very large, we can also
// prepend, or create a standalone key:offset sparseIndex for fast access.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"sort"
	"sync"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

// Common errors that can be returned by SSTable operations.
var (
	ErrTableClosed    = errors.New("sstable: table already closed")
	ErrInvalidKey     = errors.New("sstable: invalid key")
	ErrKeyNotFound    = errors.New("sstable: key not found")
	ErrCorruptedTable = errors.New("sstable: corrupted table data")
	ErrReadOnlyTable  = errors.New("sstable: cannot write to read-only table")
	ErrWriteError     = errors.New("sstable: records must be written in sorted order")
)

// File format constants.
const (
	magicHeader      = int64(0x53535442) // "SSTB" in hex
	magicFooter      = int64(0x454E4442) // "ENDB" in hex
	formatVersion    = int64(1)
	defaultBufSize   = 52 * 1024
	defaultIndexSize = 1024
)

// Options configures the behavior of an SSTable.
type Options struct {
	// ReadOnly opens the table in read-only mode if true.
	ReadOnly bool

	// BlockSize is the size of data blocks in bytes.
	BlockSize int

	// BufferSize is the size of the read/write buffer.
	BufferSize int
}

// sparseIndexEntry represents an entry in the sparse index.
type sparseIndexEntry struct {
	key    string
	offset int64
}

// Table represents a sorted string table.
type Table struct {
	mu     sync.RWMutex
	rw     io.ReadWriteSeeker
	buf    *ReaderWriterSeeker
	bw     recordio.BinaryWriter
	br     recordio.BinaryReader
	opts   Options
	closed bool

	sparseIndex []sparseIndexEntry

	// Track the offset where data ends and sparseIndex begins
	dataEnd int64
}

// Open creates a new SSTable using the provided ReadWriteSeeker.
func Open(rw io.ReadWriteSeeker, opts *Options) (*Table, error) {
	if rw == nil {
		return nil, errors.New("sstable: ReadWriteSeeker cannot be nil")
	}

	if opts == nil {
		opts = &Options{}
	}

	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}

	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufSize
	}

	buf := NewReadWriteSeeker(rw, opts.BufferSize)

	t := &Table{
		rw:          rw,
		opts:        *opts,
		sparseIndex: make([]sparseIndexEntry, 0, defaultIndexSize),
		buf:         buf,
		bw:          recordio.NewBinaryWriter(buf),
		br:          recordio.NewBinaryReader(buf),
	}

	// Check if the ReadWriteSeeker has existing content
	if size, err := rw.Seek(0, io.SeekEnd); err == nil && size > 0 {
		if err := t.loadTable(); err != nil {
			return nil, errors.Join(err, t.Close())
		}
	} else {
		// New table, write header
		if err := t.writeHeader(); err != nil {
			return nil, errors.Join(err, t.Close())
		}
		t.dataEnd = int64(binary.Size(magicHeader) + binary.Size(formatVersion))
	}

	return t, nil
}

// OpenFile opens or creates an SSTable at the given path.
func OpenFile(path string, opts *Options) (*Table, error) {
	if opts == nil {
		opts = &Options{}
	}

	flag := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flag, 0o666)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open file: %w", err)
	}

	t, err := Open(file, opts)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}

	return t, nil
}

// Close closes the table.
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	if err := t.buf.Flush(); err != nil {
		return fmt.Errorf("sstable: flush error: %w", err)
	}

	// If the underlying reader/writer is also an io.Closer, close it
	if closer, ok := t.rw.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return fmt.Errorf("sstable: close error: %w", err)
		}
	}

	return nil
}

func (t *Table) writeRecord(record partition.Record) error {
	if t.closed {
		return ErrTableClosed
	}

	if t.opts.ReadOnly {
		return ErrReadOnlyTable
	}

	// Ensure records are written in sorted order.
	if len(t.sparseIndex) > 0 {
		lastKey := t.sparseIndex[len(t.sparseIndex)-1].key
		if record.GetID() < lastKey {
			return ErrWriteError
		}
	}

	// Write record
	n, err := recordio.Write(t.buf, record)
	if err != nil {
		return err
	}

	// Update sparseIndex
	t.sparseIndex = append(t.sparseIndex, sparseIndexEntry{
		key:    record.GetID(),
		offset: t.dataEnd,
	})

	// Update data end position
	t.dataEnd += n

	return nil
}

// Get retrieves a record by its key.
func (t *Table) Get(key string) (partition.Record, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return nil, ErrTableClosed
	}

	offset, ok := t.getKeyOffset(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Seek to record position
	if _, err := t.buf.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("sstable: seek error: %w", err)
	}

	// Read and parse record directly from buffer
	record, err := recordio.ReadRecord(t.buf)
	if err != nil {
		return nil, fmt.Errorf("sstable: record parse error: %w", err)
	}

	return record, nil
}

func (t *Table) getKeyOffset(key string) (int64, bool) {
	// Binary search over sparse index
	i := sort.Search(len(t.sparseIndex), func(i int) bool {
		return t.sparseIndex[i].key >= key
	})
	if i < len(t.sparseIndex) && t.sparseIndex[i].key == key {
		return t.sparseIndex[i].offset, true
	}
	return 0, false
}

// loadTable reads the table format and loads the sparseIndex.
func (t *Table) loadTable() error {
	var (
		err error
	)

	if err := t.checkHeader(); err != nil {
		return err
	}

	indexOffset, err := t.extractIndexOffset()
	if err != nil {
		return err
	}

	err = t.readSparseIndex(indexOffset)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) readSparseIndex(indexOffset int64) error {
	// Read sparseIndex
	t.dataEnd = indexOffset
	if _, err := t.buf.Seek(indexOffset, io.SeekStart); err != nil {
		return err
	}

	count, err := t.br.ReadInt64()
	if err != nil {
		return fmt.Errorf("sstable: invalid sparseIndex count: %w", err)
	}

	t.sparseIndex = make([]sparseIndexEntry, 0, count)
	for i := int64(0); i < count; i++ {
		key, err := t.br.ReadString()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex key: %w", err)
		}

		offset, err := t.br.ReadInt64()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex offset: %w", err)
		}

		t.sparseIndex = append(t.sparseIndex, sparseIndexEntry{
			key:    key,
			offset: offset,
		})
	}
	return nil
}

func (t *Table) checkHeader() error {
	var (
		header  int64
		version int64
		err     error
	)
	_, err = t.buf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if header, err = t.br.ReadInt64(); err != nil {
		return fmt.Errorf("sstable: invalid header: %w", err)
	}
	if header != magicHeader {
		return ErrCorruptedTable
	}

	// Read version
	if version, err = t.br.ReadInt64(); err != nil {
		return fmt.Errorf("sstable: invalid version: %w", err)
	}
	if version != formatVersion {
		return fmt.Errorf("sstable: unsupported version %d", version)
	}

	return nil
}

func (t *Table) extractIndexOffset() (int64, error) {
	// Read sparseIndex offset from footer
	var indexOffset int64
	var err error

	footerSize := int64(binary.Size(indexOffset) + binary.Size(magicFooter))
	if _, err = t.buf.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, err
	}

	if indexOffset, err = t.br.ReadInt64(); err != nil {
		return 0, err
	}

	// Verify footer magic
	var footer int64
	if footer, err = t.br.ReadInt64(); err != nil {
		return 0, err
	}
	if footer != magicFooter {
		return 0, ErrCorruptedTable
	}

	return indexOffset, nil
}

// writeHeader writes the SSTable header.
func (t *Table) writeHeader() error {
	if err := binary.Write(t.buf, binary.LittleEndian, magicHeader); err != nil {
		return err
	}
	if err := binary.Write(t.buf, binary.LittleEndian, formatVersion); err != nil {
		return err
	}
	return t.buf.Flush()
}

// writeIndex writes the current sparseIndex.
func (t *Table) writeIndex() error {
	if _, err := t.bw.WriteInt64(int64(len(t.sparseIndex))); err != nil {
		return err
	}

	for _, v := range t.sparseIndex {
		if _, err := t.bw.WriteString(v.key); err != nil {
			return err
		}
		if _, err := t.bw.WriteInt64(v.offset); err != nil {
			return err
		}
	}

	// Write footer with sparseIndex offset and magic number
	if _, err := t.bw.WriteInt64(t.dataEnd); err != nil {
		return err
	}
	if _, err := t.bw.WriteInt64(magicFooter); err != nil {
		return err
	}

	return t.buf.Flush()
}

func (t *Table) All() (iter.Seq[partition.Record], error) {
	if err := t.checkHeader(); err != nil {
		return nil, err
	}

	return recordio.Seq(t.buf), nil
}

// BatchWriter creates a new BatchWriter instance.
func (t *Table) BatchWriter() *BatchWriter {
	return &BatchWriter{
		table: t,
	}
}

// BatchWriter provides functionality to write multiple records to an SSTable in batches.
type BatchWriter struct {
	table *Table
}

// Add adds a record to the batch. If the batch size reaches maxBatch,
// it automatically flushes the records to the table.
func (bw *BatchWriter) Add(record partition.Record) error {
	if record == nil {
		return ErrInvalidKey
	}

	return bw.table.writeRecord(record)
}

// AddAll adds multiple records to the batch, automatically flushing when needed.
func (bw *BatchWriter) AddAll(records []partition.Record) error {
	for _, record := range records {
		if err := bw.Add(record); err != nil {
			return fmt.Errorf("sstable: batch add error: %w", err)
		}
	}
	return nil
}

// Flush writes all buffered records to the table in sorted order.
func (bw *BatchWriter) Flush() error {
	return bw.table.writeIndex()
}

// Close flushes any remaining records and releases resources.
func (bw *BatchWriter) Close() error {
	return bw.Flush()
}
