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
)

// File format constants.
const (
	magicHeader    = int64(0x53535442) // "SSTB" in hex
	magicFooter    = int64(0x454E4442) // "ENDB" in hex
	formatVersion  = int64(1)
	defaultBufSize = 52 * 1024
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

	sparseIndex map[string]sparseIndexEntry

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
		sparseIndex: make(map[string]sparseIndexEntry),
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

// put adds or updates a record in the table.
func (t *Table) put(record partition.Record) error {
	if record == nil {
		return ErrInvalidKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	// Seek to end of data section
	if _, err := t.buf.Seek(t.dataEnd, io.SeekStart); err != nil {
		return fmt.Errorf("sstable: seek error: %w", err)
	}

	if err := t.writeRecord(record); err != nil {
		return err
	}

	if err := t.writeIndex(); err != nil {
		return fmt.Errorf("sstable: sparseIndex write error: %w", err)
	}

	return t.buf.Flush()
}

func (t *Table) writeRecord(record partition.Record) error {
	if t.closed {
		return ErrTableClosed
	}

	if t.opts.ReadOnly {
		return ErrReadOnlyTable
	}

	// Write record
	n, err := recordio.Write(t.buf, record)
	if err != nil {
		return err
	}

	// Update sparseIndex
	t.sparseIndex[record.GetID()] = sparseIndexEntry{
		key:    record.GetID(),
		offset: t.dataEnd,
	}

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

	offset, ok := t.sparseIndex[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Seek to record position
	if _, err := t.buf.Seek(offset.offset, 0); err != nil {
		return nil, fmt.Errorf("sstable: seek error: %w", err)
	}

	// Read and parse record directly from buffer
	record, err := recordio.ReadRecord(t.buf)
	if err != nil {
		return nil, fmt.Errorf("sstable: record parse error: %w", err)
	}

	return record, nil
}

// loadTable reads the table format and loads the sparseIndex.
func (t *Table) loadTable() error {
	var (
		err error
	)

	_, err = t.buf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	if err := t.checkHeader(); err != nil {
		return err

	}

	indexOffset, err := t.extractIndexOffset()
	if err != nil {
		return err
	}

	err = t.readMemTable(indexOffset)
	if err != nil {
		return err
	}

	return nil
}

func (t *Table) readMemTable(indexOffset int64) error {
	// Read sparseIndex
	t.dataEnd = indexOffset
	if _, err := t.buf.Seek(indexOffset, io.SeekStart); err != nil {
		return err
	}

	count, err := t.br.ReadInt64()
	if err != nil {
		return fmt.Errorf("sstable: invalid sparseIndex count: %w", err)
	}

	t.sparseIndex = make(map[string]sparseIndexEntry, count)
	for i := int64(0); i < count; i++ {
		key, err := t.br.ReadString()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex key: %w", err)
		}

		offset, err := t.br.ReadInt64()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex offset: %w", err)
		}

		t.sparseIndex[key] = sparseIndexEntry{
			key:    key,
			offset: offset,
		}
	}
	return nil
}

func (t *Table) checkHeader() error {
	var (
		header  int64
		version int64
		err     error
	)
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

func (t *Table) All() iter.Seq[partition.Record] {
	return func(yield func(partition.Record) bool) {
		i := t.Iter()
		for {
			record, ok := i.Next()
			if !ok {
				return
			}
			if !yield(record) {
				return
			}
		}
	}
}

// Iterator provides sequential access to table records.
type Iterator struct {
	table *Table
	keys  []string
	pos   int
}

// Iter returns an iterator over the table's records in key order.
func (t *Table) Iter() *Iterator {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keys := make([]string, 0, len(t.sparseIndex))
	for k := range t.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	return &Iterator{
		table: t,
		keys:  keys,
		pos:   0,
	}
}

// Next returns the next record in the iteration.
func (it *Iterator) Next() (partition.Record, bool) {
	if it.pos >= len(it.keys) {
		return nil, false
	}

	key := it.keys[it.pos]
	it.pos++

	record, err := it.table.Get(key)
	if err != nil {
		return nil, false
	}

	return record, true
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
	if err := bw.table.writeIndex(); err != nil {
		return fmt.Errorf("sstable: index write error: %w", err)
	}

	return bw.table.buf.Flush()
}

// Close flushes any remaining records and releases resources.
func (bw *BatchWriter) Close() error {
	return bw.Flush()
}
