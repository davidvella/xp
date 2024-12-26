package sstable_test

import (
	"bytes"
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

func TestTableBasicOperations(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Test table creation
	table, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Test single record write and read
	record := newTestRecord("key1", []byte("value1"))
	if err := table.Put(record); err != nil {
		t.Errorf("Failed to write record: %v", err)
	}

	got, err := table.Get("key1")
	if err != nil {
		t.Errorf("Failed to read record: %v", err)
	}

	if !bytes.Equal(got.GetData(), record.GetData()) {
		t.Errorf("Got %q, want %q", got.GetData(), record.GetData())
	}
}

func TestTableMultipleRecords(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	table, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Write multiple records
	records := []partition.Record{
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
		newTestRecord("key3", []byte("value3")),
	}

	for _, r := range records {
		if err := table.Put(r); err != nil {
			t.Errorf("Failed to write record: %v", err)
		}
	}

	// Read and verify all records
	for _, want := range records {
		got, err := table.Get(want.GetID())
		if err != nil {
			t.Errorf("Failed to read record %q: %v", want.GetID(), err)
			continue
		}

		if !bytes.Equal(got.GetData(), want.GetData()) {
			t.Errorf("Record %q: got %q, want %q", want.GetID(), got.GetData(), want.GetData())
		}
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
		if err := table1.Put(r); err != nil {
			t.Errorf("Failed to write record: %v", err)
		}
	}

	if err := table1.Close(); err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

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
		if err != nil {
			t.Errorf("Failed to read record %q after reopen: %v", want.GetID(), err)
			continue
		}

		if !bytes.Equal(got.GetData(), want.GetData()) {
			t.Errorf("Record %q after reopen: got %q, want %q",
				want.GetID(), got.GetData(), want.GetData())
		}
	}
}

func TestTableIterator(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	table, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Write records in random order
	records := []partition.Record{
		newTestRecord("key3", []byte("value3")),
		newTestRecord("key1", []byte("value1")),
		newTestRecord("key2", []byte("value2")),
	}

	for _, r := range records {
		if err := table.Put(r); err != nil {
			t.Errorf("Failed to write record: %v", err)
		}
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

		if record.GetID() != expectedKeys[i] {
			t.Errorf("Iterator record %d: got key %q, want %q",
				i, record.GetID(), expectedKeys[i])
		}
		i++
	}

	if i != len(expectedKeys) {
		t.Errorf("Iterator returned %d records, want %d", i, len(expectedKeys))
	}
}

func TestTableReadOnly(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	// Create and populate table
	table1, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	record := newTestRecord("key1", []byte("value1"))
	if err := table1.Put(record); err != nil {
		t.Errorf("Failed to write record: %v", err)
	}

	if err := table1.Close(); err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

	// Reopen in read-only mode
	table2, err := sstable.Open(path, &sstable.Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to open table in read-only mode: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table2)

	// Verify read works
	got, err := table2.Get("key1")
	if err != nil {
		t.Errorf("Failed to read record in read-only mode: %v", err)
	}

	if !bytes.Equal(got.GetData(), record.GetData()) {
		t.Errorf("Read-only record: got %q, want %q", got.GetData(), record.GetData())
	}

	// Verify write fails
	err = table2.Put(newTestRecord("key2", []byte("value2")))
	if err == nil {
		t.Error("Write succeeded in read-only mode")
	}
}

func TestTableErrors(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "test.sst")

	table, err := sstable.Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer func(table *sstable.Table) {
		err := table.Close()
		assert.NoError(t, err)
	}(table)

	// Test nil record
	if err := table.Put(nil); err == nil {
		t.Error("Expected error for nil record")
	}

	// Test non-existent key
	if _, err := table.Get("nonexistent"); err == nil {
		t.Error("Expected error for non-existent key")
	}

	// Test closed table operations
	err = table.Close()
	assert.NoError(t, err)

	if err := table.Put(newTestRecord("key", []byte("value"))); err == nil {
		t.Error("Write succeeded on closed table")
	}

	if _, err := table.Get("key"); err == nil {
		t.Error("Read succeeded on closed table")
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