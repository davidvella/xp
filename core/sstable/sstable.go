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
// have a sorted memtable. Optionally, if the file is very large, we can also
// prepend, or create a standalone key:offset memtable for fast access.
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"sync"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/recordio"
)

// Common errors that can be returned by SSTable operations.
var (
	ErrTableClosed    = errors.New("sstable: table already closed")
	ErrInvalidKey     = errors.New("sstable: invalid key")
	ErrKeyNotFound    = errors.New("sstable: key not found")
	ErrCorruptedTable = errors.New("sstable: corrupted table data")
)

// File format constants.
const (
	magicHeader    = uint32(0x53535442) // "SSTB" in hex
	magicFooter    = uint32(0x454E4442) // "ENDB" in hex
	formatVersion  = uint32(1)
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

// blockOffset stores the location of a record in the file.
type blockOffset struct {
	offset int64
	size   int64
}

// Table represents a sorted string table.
type Table struct {
	mu     sync.RWMutex
	file   *os.File
	buf    *bufio.ReadWriter
	bw     recordio.BinaryWriter
	opts   Options
	closed bool

	// In-memory memtable
	memtable map[string]blockOffset

	// Track the offset where data ends and memtable begins
	dataEnd int64
}

// Open opens or creates an SSTable at the given path.
func Open(path string, opts *Options) (*Table, error) {
	if opts == nil {
		opts = &Options{}
	}

	if opts.BlockSize == 0 {
		opts.BlockSize = 4096
	}

	if opts.BufferSize == 0 {
		opts.BufferSize = defaultBufSize
	}

	flag := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flag = os.O_RDONLY
	}

	file, err := os.OpenFile(path, flag, 0o666)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open file: %w", err)
	}

	buf := bufio.NewReadWriter(
		bufio.NewReaderSize(file, opts.BufferSize),
		bufio.NewWriterSize(file, opts.BufferSize),
	)

	t := &Table{
		file:     file,
		opts:     *opts,
		memtable: make(map[string]blockOffset),
		buf:      buf,
		bw:       recordio.NewBinaryWriter(buf),
	}

	// Read existing file if not empty
	if fi, err := file.Stat(); err == nil && fi.Size() > 0 {
		if err := t.loadTable(); err != nil {
			t.Close()
			return nil, err
		}
	} else {
		// New file, write header
		if err := t.writeHeader(); err != nil {
			t.Close()
			return nil, err
		}
		t.dataEnd = int64(binary.Size(magicHeader) + binary.Size(formatVersion))
	}

	return t, nil
}

// Close closes the table file.
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

	if err := t.file.Close(); err != nil {
		return fmt.Errorf("sstable: close error: %w", err)
	}

	return nil
}

// Put adds or updates a record in the table.
func (t *Table) Put(record partition.Record) error {
	if record == nil {
		return ErrInvalidKey
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return ErrTableClosed
	}

	if t.opts.ReadOnly {
		return errors.New("sstable: cannot write to read-only table")
	}

	// Seek to end of data section
	if _, err := t.file.Seek(t.dataEnd, io.SeekStart); err != nil {
		return fmt.Errorf("sstable: seek error: %w", err)
	}

	if err := t.writeRecord(record); err != nil {
		return err
	}

	// Write updated memtable
	if err := t.writeIndex(); err != nil {
		return fmt.Errorf("sstable: memtable write error: %w", err)
	}

	return t.buf.Flush()
}

func (t *Table) writeRecord(record partition.Record) error {
	// Write record
	n, err := recordio.Write(t.buf, record)
	if err != nil {
		return err
	}

	// Update memtable
	t.memtable[record.GetID()] = blockOffset{
		offset: t.dataEnd,
		size:   n,
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

	offset, ok := t.memtable[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Read record data
	data := make([]byte, offset.size)
	if _, err := t.file.ReadAt(data, offset.offset); err != nil {
		return nil, fmt.Errorf("sstable: read error: %w", err)
	}

	// Parse record
	record, err := recordio.ReadRecord(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("sstable: record parse error: %w", err)
	}

	return record, nil
}

// loadTable reads the table format and loads the memtable.
func (t *Table) loadTable() error {
	// Read and verify header
	var header uint32
	if err := binary.Read(t.buf, binary.LittleEndian, &header); err != nil {
		return fmt.Errorf("sstable: invalid header: %w", err)
	}
	if header != magicHeader {
		return ErrCorruptedTable
	}

	// Read version
	var version uint32
	if err := binary.Read(t.buf, binary.LittleEndian, &version); err != nil {
		return fmt.Errorf("sstable: invalid version: %w", err)
	}
	if version != formatVersion {
		return fmt.Errorf("sstable: unsupported version %d", version)
	}

	// Find memtable position by reading footer from end of file
	// Read memtable offset from footer
	var indexOffset int64
	footerSize := int64(binary.Size(indexOffset) + binary.Size(magicFooter))
	if _, err := t.file.Seek(-footerSize, io.SeekEnd); err != nil {
		return err
	}

	if err := binary.Read(t.file, binary.LittleEndian, &indexOffset); err != nil {
		return err
	}

	// Verify footer magic
	var footer uint32
	if err := binary.Read(t.file, binary.LittleEndian, &footer); err != nil {
		return err
	}
	if footer != magicFooter {
		return ErrCorruptedTable
	}

	// Read memtable
	t.dataEnd = indexOffset
	if _, err := t.file.Seek(indexOffset, io.SeekStart); err != nil {
		return err
	}

	br := recordio.NewBinaryReader(t.file)
	count, err := br.ReadInt64()
	if err != nil {
		return fmt.Errorf("sstable: invalid memtable count: %w", err)
	}

	t.memtable = make(map[string]blockOffset, count)
	for i := int64(0); i < count; i++ {
		key, err := br.ReadString()
		if err != nil {
			return fmt.Errorf("sstable: invalid memtable key: %w", err)
		}

		offset, err := br.ReadInt64()
		if err != nil {
			return fmt.Errorf("sstable: invalid memtable offset: %w", err)
		}

		size, err := br.ReadInt64()
		if err != nil {
			return fmt.Errorf("sstable: invalid memtable size: %w", err)
		}

		t.memtable[key] = blockOffset{
			offset: offset,
			size:   size,
		}
	}

	return nil
}

// writeHeader writes the SSTable file header.
func (t *Table) writeHeader() error {
	if err := binary.Write(t.buf, binary.LittleEndian, magicHeader); err != nil {
		return err
	}
	if err := binary.Write(t.buf, binary.LittleEndian, formatVersion); err != nil {
		return err
	}
	return t.buf.Flush()
}

// writeIndex writes the current memtable to the file.
func (t *Table) writeIndex() error {
	if _, err := t.bw.WriteInt64(int64(len(t.memtable))); err != nil {
		return err
	}

	for k, offset := range t.memtable {
		if _, err := t.bw.WriteString(k); err != nil {
			return err
		}
		if _, err := t.bw.WriteInt64(offset.offset); err != nil {
			return err
		}
		if _, err := t.bw.WriteInt64(offset.size); err != nil {
			return err
		}
	}

	// Write footer with memtable offset and magic number
	if _, err := t.bw.WriteInt64(t.dataEnd); err != nil {
		return err
	}
	if err := binary.Write(t.buf, binary.LittleEndian, magicFooter); err != nil {
		return err
	}

	return t.buf.Flush()
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

	keys := make([]string, 0, len(t.memtable))
	for k := range t.memtable {
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
