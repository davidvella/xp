package sstable_test

import (
	cyrptoRand "crypto/rand"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRecord creates a new partition.Record instance for testing.
func newTestRecord(id string, data []byte) partition.Record {
	return partition.RecordImpl{
		ID:           id,
		PartitionKey: "test-partition",
		Timestamp:    time.Now(),
		Data:         data,
	}
}

// setupWriter initializes a new SSTable writer for testing and returns the writer and a cleanup function.
func setupWriter(t *testing.T, path string) (table *sstable.TableWriter, cleanup func()) {
	t.Helper()

	writer, err := sstable.OpenWriterFile(path, nil)
	require.NoError(t, err)

	cleanup = func() {
		require.NoError(t, writer.Close())
		os.Remove(path)
	}

	return writer, cleanup
}

// setupWriter initializes a new SSTable writer for testing and returns the writer and a cleanup function.
func setupBWriter(b *testing.B, path string, opts *sstable.Options) (table *sstable.TableWriter, cleanup func()) {
	b.Helper()

	writer, err := sstable.OpenWriterFile(path, opts)
	require.NoError(b, err)

	cleanup = func() {
		require.NoError(b, writer.Close())
		os.Remove(path)
	}

	return writer, cleanup
}

// setupReader initializes a new SSTable reader for testing and returns the reader and a cleanup function.
func setupReader(t *testing.T, path string) (table *sstable.TableReader, cleanup func()) {
	t.Helper()

	reader, err := sstable.OpenReaderFile(path, nil)
	require.NoError(t, err)

	cleanup = func() {
		require.NoError(t, reader.Close())
		os.Remove(path)
	}

	return reader, cleanup
}

// setupReader initializes a new SSTable reader for testing and returns the reader and a cleanup function.
func setupBReader(b *testing.B, path string, opts *sstable.Options) (table *sstable.TableReader, cleanup func()) {
	b.Helper()

	reader, err := sstable.OpenReaderFile(path, opts)
	require.NoError(b, err)

	cleanup = func() {
		require.NoError(b, reader.Close())
		os.Remove(path)
	}

	return reader, cleanup
}

func TestTableBasicOperationsReadWriteSeeker(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_basic.sst")

	// Open writer
	writer, cleanupWriter := setupWriter(t, path)
	defer cleanupWriter()

	// Write a single record
	record := newTestRecord("key1", []byte("value1"))
	err := writer.Write(record)
	assert.NoError(t, err)

	// Close writer
	require.NoError(t, writer.Close())

	// Open reader
	reader, cleanupReader := setupReader(t, path)
	defer cleanupReader()

	// Read the single record
	got, err := reader.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, got.GetData(), record.GetData())
}

func TestHandleErrorWhenDirectoryNotExists(t *testing.T) {
	// Attempt to open a reader and writer for a non-existent directory
	nonExistentPath := "/imabadpath/nonexistent.sst"

	_, err := sstable.OpenReaderFile(nonExistentPath, nil)
	assert.Error(t, err)

	_, err = sstable.OpenWriterFile(nonExistentPath, &sstable.Options{BufferSize: 1024})
	assert.Error(t, err)
}

func TestHandleInvalidFile(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "sstable-test-*.sst")
	assert.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	p := tmpFile.Name()
	_, err = tmpFile.WriteString("im a bad file")
	assert.NoError(t, err)
	assert.NoError(t, tmpFile.Close())

	// Attempt to open reader and writer on invalid file
	_, err = sstable.OpenReaderFile(p, nil)
	assert.Error(t, err)
}

func TestTableBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_basic_ops.sst")

	// Open writer
	writer, cleanupWriter := setupWriter(t, path)
	defer cleanupWriter()

	// Write a single record
	record := newTestRecord("key1", []byte("value1"))
	err := writer.Write(record)
	assert.NoError(t, err)

	// Attempt to add an out-of-order record
	record2 := newTestRecord("key0", []byte("value0")) // Out of order
	err = writer.Write(record2)
	assert.ErrorIs(t, err, sstable.ErrWriteError)

	// Close writer
	require.NoError(t, writer.Close())

	// Open reader
	reader, cleanupReader := setupReader(t, path)
	defer cleanupReader()

	// Read the first record
	got, err := reader.Get("key1")
	assert.NoError(t, err)
	assert.Equal(t, got.GetData(), record.GetData())
}

func TestTableMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_multiple_records.sst")

	// Open writer
	writer, cleanupWriter := setupWriter(t, path)
	defer cleanupWriter()

	// Write multiple records in sorted order
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
		newTestRecord("key3", []byte("value3")),
	}

	for _, r := range records {
		err := writer.Write(r)
		assert.NoError(t, err)
	}

	// Close writer
	require.NoError(t, writer.Close())

	// Open reader
	reader, cleanupReader := setupReader(t, path)
	defer cleanupReader()

	// Read and verify all records
	for _, want := range records {
		got, err := reader.Get(want.GetID())
		assert.NoError(t, err)
		assert.Equal(t, got.GetData(), want.GetData())
	}
}

func TestTableReopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_reopen.sst")

	// Open writer
	writer1, cleanupWriter1 := setupWriter(t, path)
	defer cleanupWriter1()

	// Write records
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	for _, r := range records {
		err := writer1.Write(r)
		assert.NoError(t, err)
	}

	// Close writer
	require.NoError(t, writer1.Close())

	// Open reader
	reader2, cleanupReader2 := setupReader(t, path)
	defer cleanupReader2()

	// Verify records
	for _, want := range records {
		got, err := reader2.Get(want.GetID())
		assert.NoError(t, err)
		assert.Equal(t, got.GetData(), want.GetData())
	}
}

func TestTableErrors(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_errors.sst")

	// Open writer
	writer, cleanupWriter := setupWriter(t, path)
	defer cleanupWriter()

	// Attempt to add a nil record
	assert.Error(t, writer.Write(nil))

	// Close writer
	require.NoError(t, writer.Close())

	// Open reader
	reader, cleanupReader := setupReader(t, path)
	defer cleanupReader()

	// Test non-existent key
	_, err := reader.Get("nonexistent")
	assert.Error(t, err)

	// Test closed table operations
	require.NoError(t, writer.Close())
	require.NoError(t, reader.Close())

	// Attempt to write after closing
	err = writer.Write(newTestRecord("key", []byte("value")))
	assert.Error(t, err)

	// Attempt to read after closing
	_, err = reader.Get("key")
	assert.Error(t, err)

	// Attempt to open reader and writer with nil
	_, err = sstable.OpenReader(nil, nil)
	assert.Error(t, err)

	_, err = sstable.OpenWriter(nil, &sstable.Options{BufferSize: 1024})
	assert.Error(t, err)
}

func TestTableAll(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test_all.sst")

	// Open writer
	writer, cleanupWriter := setupWriter(t, path)
	defer cleanupWriter()

	// Write records in sorted order
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
		newTestRecord("key3", []byte("value3")),
	}

	for _, r := range records {
		assert.NoError(t, writer.Write(r))
	}

	// Close writer
	require.NoError(t, writer.Close())

	// Open reader
	reader, cleanupReader := setupReader(t, path)
	defer cleanupReader()

	// Iterate over all records
	iter, err := reader.All()
	assert.NoError(t, err)

	i := 0
	expectedKeys := []string{"key1", "key2", "key3"}

	for record := range iter {
		assert.Equal(t, record.GetID(), expectedKeys[i])
		assert.Equal(t, record.GetData(), records[i].GetData())
		i++
	}
}

func generateMockRecords(count int) []partition.Record {
	records := make([]partition.Record, count)
	for i := 0; i < count; i++ {
		records[i] = &partition.RecordImpl{
			ID:   fmt.Sprintf("key-%06d", i),
			Data: []byte(fmt.Sprintf("value-%06d", i)),
		}
	}
	return records
}

func BenchmarkTableWrite(b *testing.B) {
	benchCases := []struct {
		name      string
		batchSize int
	}{
		{"Small", 10000},
		{"Medium", 100000},
		{"Large", 1000000},
	}

	for _, bc := range benchCases {
		records := generateMockRecords(bc.batchSize)

		b.Run(fmt.Sprintf("%s/SingleWrite/%d", bc.name, bc.batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				path := filepath.Join(b.TempDir(), fmt.Sprintf("bench-%d.sst", i))
				writer, err := sstable.OpenWriterFile(path, nil)
				require.NoError(b, err)

				for _, record := range records {
					require.NoError(b, writer.Write(record))
				}

				require.NoError(b, writer.Close())
			}
		})
	}
}

func BenchmarkTableRead(b *testing.B) {
	tmpDir := os.TempDir()
	path := filepath.Join(tmpDir, "bench_read.sst")

	// Open writer
	writer, err := sstable.OpenWriterFile(path, nil)
	require.NoError(b, err)

	record := newTestRecord("benchkey", []byte("benchvalue"))

	err = writer.Write(record)
	require.NoError(b, err)

	require.NoError(b, writer.Close())

	// Open reader
	reader, err := sstable.OpenReaderFile(path, nil)
	require.NoError(b, err)
	defer reader.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := reader.Get("benchkey")
		if err != nil {
			b.Errorf("Read failed: %v", err)
		}
	}
}

func BenchmarkTableRandomRead(b *testing.B) {
	benchCases := []struct {
		name      string
		tableSize int // Number of records to populate the table with
	}{
		{"Small", 1000},
		{"Medium", 10000},
		{"Large", 100000},
	}

	for _, bc := range benchCases {
		b.Run(fmt.Sprintf("TableSize_%d", bc.tableSize), func(b *testing.B) {
			// Set up table
			tmpDir := b.TempDir()
			path := filepath.Join(tmpDir, "bench_random_read.sst")

			writer, cleanupWriter := setupBWriter(b, path, nil)
			defer cleanupWriter()

			// Generate and insert records
			records := generateMockRecords(bc.tableSize)
			for _, r := range records {
				err := writer.Write(r)
				require.NoError(b, err)
			}
			require.NoError(b, writer.Close())

			// Open reader
			reader, cleanupReader := setupBReader(b, path, nil)
			defer cleanupReader()

			// Pre-generate random keys
			readKeys := make([]string, b.N)
			for i := 0; i < b.N; i++ {
				//nolint:gosec // Don't need crypto security in test
				idx := rand.Intn(bc.tableSize)
				readKeys[i] = fmt.Sprintf("key-%06d", idx)
			}

			// Benchmark random reads
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := reader.Get(readKeys[i])
				if err != nil {
					b.Errorf("Read failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkIndividualWrites(b *testing.B) {
	benchCases := []struct {
		name      string
		valueSize int // Size of the value in bytes
	}{
		{"SmallValue", 64},
		{"MediumValue", 1024},
		{"LargeValue", 1024 * 1024}, // 1MB
	}

	for _, bc := range benchCases {
		b.Run(bc.name, func(b *testing.B) {
			path := filepath.Join(b.TempDir(), "bench_individual_write.sst")
			writer, cleanupWriter := setupBWriter(b, path, nil)
			defer cleanupWriter()

			value := make([]byte, bc.valueSize)
			_, err := cyrptoRand.Read(value)
			require.NoError(b, err)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				record := newTestRecord(
					fmt.Sprintf("key-%09d", i), // Ensure keys are sorted
					value,
				)
				if err := writer.Write(record); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
		})
	}
}
