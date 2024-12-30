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
	"bufio"
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
	footerSize        = int64(binary.Size(magicHeader) + binary.Size(formatVersion))
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

// TableReader represents the reading component of an SSTable.
type TableReader struct {
	mu          sync.RWMutex
	rw          io.ReadSeeker
	buf         *BufferReaderSeeker
	br          recordio.BinaryReader
	opts        Options
	closed      bool
	sparseIndex []sparseIndexEntry
	dataEnd     int64
}

// TableWriter represents the writing component of an SSTable.
type TableWriter struct {
	mu          sync.Mutex
	rw          io.Writer
	buf         *bufio.Writer
	bw          recordio.BinaryWriter
	opts        Options
	closed      bool
	sparseIndex []sparseIndexEntry
	dataEnd     int64
}

// OpenWriter initializes a new SSTableWriter using the provided WriteSeeker.
func OpenWriter(rw io.WriteSeeker, opts *Options) (*TableWriter, error) {
	if rw == nil {
		return nil, errors.New("sstable: WriteSeeker cannot be nil")
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

	buf := bufio.NewWriterSize(rw, defaultBufSize)
	writer := &TableWriter{
		rw:          rw,
		opts:        *opts,
		sparseIndex: make([]sparseIndexEntry, 0, defaultIndexSize),
		bw:          recordio.NewBinaryWriter(buf),
		buf:         buf,
	}

	// Write header for new table
	if err := writer.writeHeader(); err != nil {
		return nil, fmt.Errorf("sstable: failed to write header: %w", err)
	}

	writer.dataEnd = footerSize

	return writer, nil
}

// OpenReader initializes a new SSTableReader using the provided ReadSeeker.
func OpenReader(rs io.ReadSeeker, opts *Options) (*TableReader, error) {
	if rs == nil {
		return nil, errors.New("sstable: ReadSeeker cannot be nil")
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

	reader := &TableReader{
		rw:          rs,
		opts:        *opts,
		sparseIndex: make([]sparseIndexEntry, 0, defaultIndexSize),
		buf:         NewReadSeeker(rs, defaultBufSize),
		br:          recordio.NewBinaryReader(rs),
	}

	// Check if the ReadSeeker has existing content
	if size, err := rs.Seek(0, io.SeekEnd); err == nil && size > footerSize {
		if err := reader.loadTable(); err != nil {
			return nil, fmt.Errorf("sstable: failed to load table: %w", err)
		}
	} else {
		return nil, errors.New("sstable: file is empty or corrupted")
	}

	return reader, nil
}

// OpenWriterFile opens or creates an SSTable writer at the given path.
func OpenWriterFile(path string, opts *Options) (*TableWriter, error) {
	if opts == nil {
		opts = &Options{}
	}

	flag := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		return nil, errors.New("sstable: cannot open writer in read-only mode")
	}

	file, err := os.OpenFile(path, flag, 0o666)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open file for writing: %w", err)
	}

	writer, err := OpenWriter(file, opts)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("sstable: failed to initialize writer: %w", err)
	}

	return writer, nil
}

// OpenReaderFile opens an existing SSTable reader at the given path.
func OpenReaderFile(path string, opts *Options) (*TableReader, error) {
	if opts == nil {
		opts = &Options{}
	}

	flag := os.O_RDONLY

	file, err := os.OpenFile(path, flag, 0o666)
	if err != nil {
		return nil, fmt.Errorf("sstable: failed to open file for reading: %w", err)
	}

	reader, err := OpenReader(file, opts)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("sstable: failed to initialize reader: %w", err)
	}

	return reader, nil
}

func (w *TableWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	return w.writeIndex()
}

// Close closes the reader component.
func (r *TableReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	r.closed = true

	return nil
}

func (r *TableReader) loadTable() error {
	if err := r.checkHeader(); err != nil {
		return err
	}

	indexOffset, err := r.extractIndexOffset()
	if err != nil {
		return err
	}

	err = r.readSparseIndex(indexOffset)
	if err != nil {
		return err
	}

	return nil
}

func (r *TableReader) readSparseIndex(indexOffset int64) error {
	// Read sparseIndex
	r.dataEnd = indexOffset
	if _, err := r.buf.Seek(indexOffset, io.SeekStart); err != nil {
		return err
	}

	count, err := r.br.ReadInt64()
	if err != nil {
		return fmt.Errorf("sstable: invalid sparseIndex count: %w", err)
	}

	r.sparseIndex = make([]sparseIndexEntry, 0, count)
	for i := int64(0); i < count; i++ {
		key, err := r.br.ReadString()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex key: %w", err)
		}

		offset, err := r.br.ReadInt64()
		if err != nil {
			return fmt.Errorf("sstable: invalid sparseIndex offset: %w", err)
		}

		r.sparseIndex = append(r.sparseIndex, sparseIndexEntry{
			key:    key,
			offset: offset,
		})
	}
	return nil
}

func (r *TableReader) checkHeader() error {
	var (
		header  int64
		version int64
		err     error
	)
	_, err = r.buf.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}
	if header, err = r.br.ReadInt64(); err != nil {
		return fmt.Errorf("sstable: invalid header: %w", err)
	}
	if header != magicHeader {
		return ErrCorruptedTable
	}

	// Read version
	if version, err = r.br.ReadInt64(); err != nil {
		return fmt.Errorf("sstable: invalid version: %w", err)
	}
	if version != formatVersion {
		return fmt.Errorf("sstable: unsupported version %d", version)
	}

	return nil
}

// extractIndexOffset reads the index offset from the footer.
func (r *TableReader) extractIndexOffset() (int64, error) {
	// Read sparseIndex offset from footer
	var indexOffset int64
	var err error

	footerSize := int64(binary.Size(indexOffset) + binary.Size(magicFooter))
	if _, err = r.buf.Seek(-footerSize, io.SeekEnd); err != nil {
		return 0, err
	}

	if indexOffset, err = r.br.ReadInt64(); err != nil {
		return 0, err
	}

	// Verify footer magic
	var footer int64
	if footer, err = r.br.ReadInt64(); err != nil {
		return 0, err
	}
	if footer != magicFooter {
		return 0, ErrCorruptedTable
	}

	return indexOffset, nil
}

// Get retrieves a record by its key.
func (r *TableReader) Get(key string) (partition.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return nil, ErrTableClosed
	}

	offset, ok := r.getKeyOffset(key)
	if !ok {
		return nil, ErrKeyNotFound
	}

	// Seek to record position
	if _, err := r.buf.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("sstable: seek error: %w", err)
	}

	// Read and parse record directly from buffer
	record, err := recordio.ReadRecord(r.buf)
	if err != nil {
		return nil, fmt.Errorf("sstable: record parse error: %w", err)
	}

	return record, nil
}

// getKeyOffset performs a binary search over the sparse index to find the key's offset.
func (r *TableReader) getKeyOffset(key string) (int64, bool) {
	// Binary search over sparse index
	i := sort.Search(len(r.sparseIndex), func(i int) bool {
		return r.sparseIndex[i].key >= key
	})
	if i < len(r.sparseIndex) && r.sparseIndex[i].key == key {
		return r.sparseIndex[i].offset, true
	}
	return 0, false
}

// All returns an iterator over all records in the table.
func (r *TableReader) All() (iter.Seq[partition.Record], error) {
	if err := r.checkHeader(); err != nil {
		return nil, err
	}

	return recordio.Seq(r.buf), nil
}

// writeHeader writes the SSTable header.
func (w *TableWriter) writeHeader() error {
	if err := binary.Write(w.buf, binary.LittleEndian, magicHeader); err != nil {
		return err
	}
	if err := binary.Write(w.buf, binary.LittleEndian, formatVersion); err != nil {
		return err
	}
	return nil
}

// writeRecord writes a single record to the table.
func (w *TableWriter) writeRecord(record partition.Record) error {
	if w.closed {
		return ErrTableClosed
	}

	if w.opts.ReadOnly {
		return ErrReadOnlyTable
	}

	// Ensure records are written in sorted order.
	if len(w.sparseIndex) > 0 {
		lastKey := w.sparseIndex[len(w.sparseIndex)-1].key
		if record.GetID() < lastKey {
			return ErrWriteError
		}
	}

	// Write record
	n, err := recordio.Write(w.buf, record)
	if err != nil {
		return err
	}

	// Update sparseIndex
	w.sparseIndex = append(w.sparseIndex, sparseIndexEntry{
		key:    record.GetID(),
		offset: w.dataEnd,
	})

	// Update data end position
	w.dataEnd += n

	return nil
}

// BatchWriter creates a new BatchWriter instance.
func (w *TableWriter) BatchWriter() *BatchWriter {
	return &BatchWriter{
		writer: w,
	}
}

// Flush writes the current sparse index to the footer.
func (w *TableWriter) Flush() error {
	return w.buf.Flush()
}

// writeIndex writes the current sparseIndex and footer.
func (w *TableWriter) writeIndex() error {
	if _, err := w.bw.WriteInt64(int64(len(w.sparseIndex))); err != nil {
		return err
	}

	for _, v := range w.sparseIndex {
		if _, err := w.bw.WriteString(v.key); err != nil {
			return err
		}
		if _, err := w.bw.WriteInt64(v.offset); err != nil {
			return err
		}
	}

	// Write footer with sparseIndex offset and magic number
	if _, err := w.bw.WriteInt64(w.dataEnd); err != nil {
		return err
	}
	if _, err := w.bw.WriteInt64(magicFooter); err != nil {
		return err
	}

	return w.buf.Flush()
}

// BatchWriter provides functionality to write multiple records to an SSTable in batches.
type BatchWriter struct {
	writer *TableWriter
}

// Add adds a record to the batch.
func (bw *BatchWriter) Add(record partition.Record) error {
	if record == nil {
		return ErrInvalidKey
	}

	return bw.writer.writeRecord(record)
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
	return bw.writer.Flush()
}

// Close flushes any remaining records and releases resources.
func (bw *BatchWriter) Close() error {
	return bw.writer.Close()
}
