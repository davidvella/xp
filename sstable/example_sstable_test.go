package sstable_test

import (
	"bytes"
	"fmt"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
)

// ExampleOpenWriter demonstrates creating an SSTable, writing a record, and reading it back.
func ExampleOpenWriter() {
	// Create an in-memory buffer to act as the file.
	var buf bytes.Buffer

	// Initialize the SSTable writer.
	writer, err := sstable.OpenWriter(&buf, nil)
	if err != nil {
		fmt.Printf("Error opening writer: %v\n", err)
		return
	}

	// Create a new record.
	record := partition.RecordImpl{
		ID:           "exampleKey",
		PartitionKey: "examplePartition",
		Timestamp:    time.Now(),
		Data:         []byte("exampleValue"),
	}

	// Write the record to the SSTable.
	if err := writer.Write(&record); err != nil {
		fmt.Printf("Error writing record: %v\n", err)
		return
	}

	// Close the writer to flush data.
	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing writer: %v\n", err)
		return
	}
	// Output:
}

// ExampleOpenReader demonstrates reading an sstable back.
func ExampleOpenReader() {
	// Create an in-memory buffer to act as the file.
	var buf bytes.Buffer

	// Initialize the SSTable writer.
	writer, err := sstable.OpenWriter(&buf, nil)
	if err != nil {
		fmt.Printf("Error opening writer: %v\n", err)
		return
	}

	// Create a new record.
	record := partition.RecordImpl{
		ID:           "exampleKey",
		PartitionKey: "examplePartition",
		Timestamp:    time.Now(),
		Data:         []byte("exampleValue"),
	}

	// Write the record to the SSTable.
	if err := writer.Write(&record); err != nil {
		fmt.Printf("Error writing record: %v\n", err)
		return
	}

	// Close the writer to flush data.
	if err := writer.Close(); err != nil {
		fmt.Printf("Error closing writer: %v\n", err)
		return
	}

	// Initialize the SSTable reader.
	readerSeeker := bytes.NewReader(buf.Bytes())
	reader, err := sstable.OpenReader(readerSeeker, nil)
	if err != nil {
		fmt.Printf("Error opening reader: %v\n", err)
		return
	}
	defer reader.Close()

	// Retrieve the record by key.
	readRecord, err := reader.Get("exampleKey")
	if err != nil {
		fmt.Printf("Error reading record: %v\n", err)
		return
	}

	// Print the retrieved record's data.
	fmt.Println(string(readRecord.GetData()))

	// Read all records
	for rec := range reader.All() {
		fmt.Println(rec.GetID())
	}
	// Output:
	// exampleValue
	// exampleKey
}
