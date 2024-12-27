package sstable_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/sstable"
	"github.com/stretchr/testify/assert"
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

	tmpFile, err := os.CreateTemp("", "sstable-test-*.sst")
	assert.NoError(t, err)

	table, err = sstable.Open(tmpFile.Name(), nil)
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

func TestTableBasicOperations(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Test single record write and read
	record := newTestRecord("key1", []byte("value1"))
	err := table.Put(record)
	assert.NoError(t, err)

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

	for _, r := range records {
		assert.NoError(t, table.Put(r))
	}

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
	table1, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	for _, r := range records {
		assert.NoError(t, table1.Put(r))
	}

	err = table1.Close()
	assert.NoError(t, err)

	// Reopen and verify
	table2, err := sstable.Open(path, nil)
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

func TestTableIterator(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	// Write records in random order
	records := []partition.Record{
		newTestRecord("key3", []byte("value3")),
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	for _, r := range records {
		assert.NoError(t, table.Put(r))
	}

	// Verify iterator returns records in sorted order
	iter := table.Iter()
	i := 0
	expectedKeys := []string{"key1", "key2", "key3"}

	for {
		record, ok := iter.Next()
		if !ok {
			break
		}

		if i >= len(expectedKeys) {
			t.Errorf("Iterator returned more records than expected")
			break
		}

		assert.Equal(t, record.GetID(), expectedKeys[i])
		i++
	}

	if i != len(expectedKeys) {
		t.Errorf("Iterator returned %d records, want %d", i, len(expectedKeys))
	}
}

func TestTableReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Write records
	table1, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	record := newTestRecord("key1", []byte("value1"))
	assert.NoError(t, table1.Put(record))

	assert.NoError(t, table1.Close())

	// Reopen in read-only mode
	table2, err := sstable.Open(path, &sstable.Options{ReadOnly: true})
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
	err = table2.Put(newTestRecord("key2", []byte("value2")))
	assert.Error(t, err)
}

func TestTableErrors(t *testing.T) {
	table, cleanup := setupTestingTable(t)
	defer cleanup()

	assert.Error(t, table.Put(nil))

	// Test non-existent key
	_, err := table.Get("nonexistent")
	assert.Error(t, err)

	// Test closed table operations
	assert.NoError(t, table.Close())

	assert.Error(t, table.Put(newTestRecord("key", []byte("value"))))

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
			&partition.RecordImpl{ID: "valid", Data: []byte("value")},
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
		newTestRecord("key3", []byte("value3")),
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	for _, r := range records {
		assert.NoError(t, table.Put(r))
	}

	i := 0
	expectedKeys := []string{"key1", "key2", "key3"}

	for record := range table.All() {
		assert.Equal(t, record.GetID(), expectedKeys[i])
		i++
	}
}

func setupBenchmarkTable(b *testing.B) (table *sstable.Table, cleanup func()) {
	b.Helper()

	tmpFile, err := os.CreateTemp("", "sstable-bench-*.sst")
	if err != nil {
		b.Fatal(err)
	}

	table, err = sstable.Open(tmpFile.Name(), nil)
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
		{"SmallBatch", 100},
		{"MediumBatch", 1000},
		{"LargeBatch", 10000},
	}

	for _, bc := range benchCases {
		records := generateMockRecords(bc.batchSize)

		b.Run(fmt.Sprintf("%s/SingleAdd/%d", bc.name, bc.batchSize), func(b *testing.B) {
			table, cleanup := setupBenchmarkTable(b)
			defer cleanup()

			writer := table.BatchWriter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, record := range records {
					if err := writer.Add(record); err != nil {
						b.Fatal(err)
					}
				}
				if err := writer.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("%s/BatchAdd/%d", bc.name, bc.batchSize), func(b *testing.B) {
			table, cleanup := setupBenchmarkTable(b)
			defer cleanup()

			writer := table.BatchWriter()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := writer.AddAll(records); err != nil {
					b.Fatal(err)
				}
				if err := writer.Flush(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkTableWrite(b *testing.B) {
	table, cleanup := setupBenchmarkTable(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := newTestRecord(
			fmt.Sprintf("key%d", i),
			[]byte(fmt.Sprintf("value%d", i)),
		)
		if err := table.Put(record); err != nil {
			b.Errorf("Write failed: %v", err)
		}
	}
}

func BenchmarkTableRead(b *testing.B) {
	table, cleanup := setupBenchmarkTable(b)
	defer cleanup()

	record := newTestRecord("benchkey", []byte("benchvalue"))
	if err := table.Put(record); err != nil {
		b.Fatalf("Failed to write test record: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := table.Get("benchkey"); err != nil {
			b.Errorf("Read failed: %v", err)
		}
	}
}
