package sstable

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
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
	table, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

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

	table, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

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
	table1, err := Open(path, nil)
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
	table2, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to reopen table: %v", err)
	}
	defer table2.Close()

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

	table, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

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
	table1, err := Open(path, nil)
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
	table2, err := Open(path, &Options{ReadOnly: true})
	if err != nil {
		t.Fatalf("Failed to open table in read-only mode: %v", err)
	}
	defer table2.Close()

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

	table, err := Open(path, nil)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

	// Test nil record
	if err := table.Put(nil); err == nil {
		t.Error("Expected error for nil record")
	}

	// Test non-existent key
	if _, err := table.Get("nonexistent"); err == nil {
		t.Error("Expected error for non-existent key")
	}

	// Test closed table operations
	table.Close()

	if err := table.Put(newTestRecord("key", []byte("value"))); err == nil {
		t.Error("Write succeeded on closed table")
	}

	if _, err := table.Get("key"); err == nil {
		t.Error("Read succeeded on closed table")
	}
}

func BenchmarkTableWrite(b *testing.B) {
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.sst")

	table, err := Open(path, nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

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
	tmpDir := b.TempDir()
	path := filepath.Join(tmpDir, "bench.sst")

	// Setup: Create table and write test data
	table, err := Open(path, nil)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

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
