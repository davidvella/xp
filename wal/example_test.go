package wal_test

import (
	"bytes"
	"fmt"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/wal"
)

type writeCloser struct {
	*bytes.Buffer
}

func (w writeCloser) Close() error {
	return nil
}

// ExampleWriter demonstrates creating a WAL, writing records, and reading them back.
func ExampleWriter() {
	buffer := bytes.NewBuffer([]byte{})
	buf := writeCloser{buffer}
	// Initialize the WAL writer with segments of up to 1000 records
	writer, err := wal.NewWriter(buf, 1000)
	if err != nil {
		fmt.Printf("Error opening writer: %v\n", err)
		return
	}

	// Create and write some records
	records := []partition.Record{
		&partition.RecordImpl{
			ID:           "key1",
			PartitionKey: "partition1",
			Timestamp:    time.Now(),
			Data:         []byte("value1"),
		},
		&partition.RecordImpl{
			ID:           "key2",
			PartitionKey: "partition1",
			Timestamp:    time.Now(),
			Data:         []byte("value2"),
		},
	}

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			fmt.Printf("Error writing record: %v\n", err)
			return
		}
	}

	// Close the writer to flush any remaining records
	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing writer: %v\n", err)
		return
	}

	// Initialize the WAL reader
	readerAt := bytes.NewReader(buf.Bytes())
	reader := wal.NewReader(readerAt)

	// Read all records back
	for record := range reader.All() {
		fmt.Printf("Read record: %s = %s\n", record.GetID(), string(record.GetData()))
	}

	// Output:
	// Read record: key1 = value1
	// Read record: key2 = value2
}

// ExampleWriter_segments demonstrates how the WAL handles segment rotation.
func ExampleWriter_segments() {
	buffer := bytes.NewBuffer([]byte{})
	buf := writeCloser{buffer}
	// Initialize the WAL writer with small segments (2 records per segment)
	writer, err := wal.NewWriter(&buf, 2)
	if err != nil {
		fmt.Printf("Error opening writer: %v\n", err)
		return
	}

	// Write three records to trigger segment rotation
	records := []partition.Record{
		&partition.RecordImpl{
			ID:           "1",
			PartitionKey: "partition1",
			Timestamp:    time.Now(),
			Data:         []byte("first"),
		},
		&partition.RecordImpl{
			ID:           "2",
			PartitionKey: "partition1",
			Timestamp:    time.Now(),
			Data:         []byte("second"),
		},
		&partition.RecordImpl{
			ID:           "3",
			PartitionKey: "partition1",
			Timestamp:    time.Now(),
			Data:         []byte("third"),
		},
	}

	for _, record := range records {
		if err := writer.Write(record); err != nil {
			fmt.Printf("Error writing record: %v\n", err)
			return
		}
	}

	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing writer: %v\n", err)
		return
	}

	// Read all records back
	readerAt := bytes.NewReader(buf.Bytes())
	reader := wal.NewReader(readerAt)
	for record := range reader.All() {
		fmt.Printf("Read record: %s = %s\n", record.GetID(), string(record.GetData()))
	}

	// Output:
	// Read record: 1 = first
	// Read record: 2 = second
	// Read record: 3 = third
}
