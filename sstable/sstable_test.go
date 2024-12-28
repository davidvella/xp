package sstable_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRecord(id string, data []byte) partition.Record {
	return partition.RecordImpl{
		ID:           id,
		PartitionKey: "test-partition",
		Timestamp:    time.Now(),
		Data:         data,
	}
}

func setupTestingTable(t *testing.T) (table *sstable.Table, cleanup func()) {
	t.Helper()

	tmpFile, err := os.CreateTemp(t.TempDir(), "sstable-test-*.sst")
	assert.NoError(t, err)

	table, err = sstable.OpenFile(tmpFile.Name(), nil)
	if err != nil {
		os.Remove(tmpFile.Name())
		t.Fatal(err)
	}

	cleanup = func() {
		table.Close()
		os.Remove(tmpFile.Name())
	}

	return table, cleanup
}

func TestTableBasicOperationsReadWriteSeeker(t *testing.T) {
	tmpFile, err := os.CreateTemp(t.TempDir(), "sstable-test-*.sst")
	assert.NoError(t, err)

	table, err := sstable.Open(tmpFile, nil)
	assert.NoError(t, err)

	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Test single record write and read
	record := newTestRecord("key1", []byte("value1"))
	writer := table.BatchWriter()
	err = writer.Add(record)
	assert.NoError(t, err)

	assert.NoError(t, writer.Close())

	got, err := table.Get("key1")
	assert.NoError(t, err)

	assert.Equal(t, got.GetData(), record.GetData())
}

func TestHandleErrorWhenDirectoryNotExists(t *testing.T) {
	_, err := sstable.OpenFile("/imabadpath", nil)
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

	_, err = sstable.OpenFile(p, nil)
	assert.Error(t, err)
}

func TestTableBasicOperations(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Test single record write and read
	record := newTestRecord("key1", []byte("value1"))
	writer := table.BatchWriter()
	err := writer.Add(record)
	assert.NoError(t, err)

	assert.NoError(t, writer.Close())

	got, err := table.Get("key1")
	assert.NoError(t, err)

	assert.Equal(t, got.GetData(), record.GetData())
}

func TestTableMultipleRecords(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	// Write multiple records
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
		newTestRecord("key3", []byte("value3")),
	}

	writer := table.BatchWriter()

	for _, r := range records {
		assert.NoError(t, writer.Add(r))
	}

	assert.NoError(t, writer.Close())

	// Read and verify all records
	for _, want := range records {
		got, err := table.Get(want.GetID())
		assert.NoError(t, err)

		assert.Equal(t, got.GetData(), want.GetData())
	}
}

func TestTableReopen(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Write records
	table1, err := sstable.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	writer := table1.BatchWriter()

	for _, r := range records {
		assert.NoError(t, writer.Add(r))
	}

	assert.NoError(t, writer.Close())

	err = table1.Close()
	assert.NoError(t, err)

	// Reopen and verify
	table2, err := sstable.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Failed to reopen table: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table2)

	for _, want := range records {
		got, err := table2.Get(want.GetID())
		assert.NoError(t, err)

		assert.Equal(t, got.GetData(), want.GetData())
	}
}

func TestTableReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Write records
	table1, err := sstable.OpenFile(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	record := newTestRecord("key1", []byte("value1"))
	writer := table1.BatchWriter()

	assert.NoError(t, writer.Add(record))

	assert.NoError(t, writer.Close())

	assert.NoError(t, table1.Close())

	// Reopen in read-only mode
	table2, err := sstable.OpenFile(path, &sstable.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to reopen table: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table2)

	// Verify read works
	got, err := table2.Get("key1")
	assert.NoError(t, err)

	assert.Equal(t, got.GetData(), record.GetData())

	// Verify write fails
	writer = table2.BatchWriter()

	assert.Error(t, writer.Add(record))
}

func TestTableErrors(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()
	writer := table.BatchWriter()

	assert.Error(t, writer.Add(nil))

	assert.NoError(t, writer.Close())

	// Test non-existent key
	_, err := table.Get("nonexistent")
	assert.Error(t, err)

	// Test closed table operations
	assert.NoError(t, table.Close())

	writer = table.BatchWriter()

	assert.Error(t, writer.Add(newTestRecord("key", []byte("value"))))

	_, err = table.Get("key")
	assert.Error(t, err)
}

func TestBatchWriter_Add(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	bw := table.BatchWriter()

	t.Run("Add single valid record", func(t *testing.T) {
		record := &partition.RecordImpl{
			ID:   "test1",
			Data: []byte("value1"),
		}

		err := bw.Add(record)
		assert.NoError(t, err)
	})

	t.Run("Add nil record", func(t *testing.T) {
		err := bw.Add(nil)
		assert.ErrorIs(t, err, sstable.ErrInvalidKey)
	})

	t.Run("Add to closed table", func(t *testing.T) {
		record := &partition.RecordImpl{
			ID:   "test2",
			Data: []byte("value2"),
		}

		err := table.Close()
		assert.NoError(t, err)
		err = bw.Add(record)
		assert.ErrorIs(t, err, sstable.ErrTableClosed)
	})
}

func TestBatchWriter_AddAll(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	bw := table.BatchWriter()

	t.Run("Add empty record slice", func(t *testing.T) {
		err := bw.AddAll([]partition.Record{})
		assert.NoError(t, err)
	})

	t.Run("Add slice with nil record", func(t *testing.T) {
		records := []partition.Record{
			&partition.RecordImpl{ID: "batch0", Data: []byte("value")},
			nil,
		}

		err := bw.AddAll(records)
		assert.ErrorIs(t, err, sstable.ErrInvalidKey)
	})

	t.Run("Add multiple valid records", func(t *testing.T) {
		records := []partition.Record{
			&partition.RecordImpl{ID: "batch1", Data: []byte("value1")},
			&partition.RecordImpl{ID: "batch2", Data: []byte("value2")},
			&partition.RecordImpl{ID: "batch3", Data: []byte("value3")},
		}

		err := bw.AddAll(records)
		assert.NoError(t, err)

		err = bw.Flush()
		assert.NoError(t, err)

		// Verify all records were written
		for _, record := range records {
			retrieved, err := table.Get(record.GetID())
			assert.NoError(t, err)
			assert.Equal(t, retrieved.GetID(), record.GetID())
		}
	})
}

func TestBatchWriter_FlushAndClose(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	bw := table.BatchWriter()

	t.Run("Flush after adding records", func(t *testing.T) {
		record := &partition.RecordImpl{
			ID:   "flush-test",
			Data: []byte("value"),
		}
		assert.NoError(t, bw.Add(record))

		assert.NoError(t, bw.Flush())
	})

	t.Run("Close batch writer", func(t *testing.T) {
		assert.NoError(t, bw.Close())
	})
}

func TestTableAll(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	// Write records in random order
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
		newTestRecord("key3", []byte("value3")),
	}

	writer := table.BatchWriter()

	for _, r := range records {
		assert.NoError(t, writer.Add(r))
	}

	assert.NoError(t, writer.Close())

	i := 0
	expectedKeys := []string{"key1", "key2", "key3"}

	iter, err := table.All()
	assert.NoError(t, err)

	for record := range iter {
		assert.Equal(t, record.GetID(), expectedKeys[i])
		i++
	}
}

func setupBenchmarkTable(b *testing.B) (table *sstable.Table, cleanup func()) {
	b.Helper()

	tmpFile, err := os.CreateTemp(b.TempDir(), "sstable-bench-*.sst")
	if err != nil {
		b.Fatal(err)
	}

	table, err = sstable.OpenFile(tmpFile.Name(), nil)
	if err != nil {
		os.Remove(tmpFile.Name())
		b.Fatal(err)
	}

	cleanup = func() {
		table.Close()
		os.Remove(tmpFile.Name())
	}

	return table, cleanup
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

func BenchmarkBatchWriter(b *testing.B) {
	benchCases := []struct {
		name      string
		batchSize int
	}{
		{"SmallBatch", 10000},
		{"MediumBatch", 100000},
		{"LargeBatch", 1000000},
	}

	for _, bc := range benchCases {
		records := generateMockRecords(bc.batchSize)

		b.Run(fmt.Sprintf("%s/SingleAdd/%d", bc.name, bc.batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				table, cleanup := setupBenchmarkTable(b)

				writer := table.BatchWriter()

				for _, record := range records {
					require.NoError(b, writer.Add(record))
				}
				require.NoError(b, writer.Flush())

				cleanup()
			}
		})

		b.Run(fmt.Sprintf("%s/BatchAdd/%d", bc.name, bc.batchSize), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				table, cleanup := setupBenchmarkTable(b)
				writer := table.BatchWriter()
				require.NoError(b, writer.AddAll(records))
				require.NoError(b, writer.Flush())
				cleanup()
			}
		})
	}
}

func BenchmarkTableRead(b *testing.B) {
	table, cleanup := setupBenchmarkTable(b)
	defer cleanup()

	record := newTestRecord("benchkey", []byte("benchvalue"))

	writer := table.BatchWriter()
	err := writer.Add(record)
	require.NoError(b, err)

	err = writer.Close()
	require.NoError(b, err)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := table.Get("benchkey"); err != nil {
			b.Errorf("Read failed: %v", err)
		}
	}
}
