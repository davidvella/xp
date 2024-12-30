package recordio_test

import (
	"bytes"
	"fmt"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/recordio"
)

// ExampleWrite demonstrates writing and reading a single record.
func ExampleWrite() {
	// Create a record
	record := &partition.RecordImpl{
		ID:           "test1",
		PartitionKey: "partition1",
		Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Data:         []byte("Hello, World!"),
	}

	// Write the record to a buffer
	var buf bytes.Buffer
	n, err := recordio.Write(&buf, record)
	if err != nil {
		fmt.Printf("Error writing record: %v\n", err)
		return
	}

	fmt.Printf("Wrote %d bytes\n", n)

	// Read the record back
	readRecord, err := recordio.ReadRecord(&buf)
	if err != nil {
		fmt.Printf("Error reading record: %v\n", err)
		return
	}

	fmt.Printf("Read record: ID=%s, Data=%s\n",
		readRecord.GetID(), readRecord.GetData())

	// Output:
	// Wrote 74 bytes
	// Read record: ID=test1, Data=Hello, World!
}

// ExampleSeq demonstrates reading multiple records using an iterator.
func ExampleSeq() {
	// Create some records
	records := []partition.Record{
		&partition.RecordImpl{
			ID:        "1",
			Timestamp: time.Now(),
			Data:      []byte("first"),
		},
		&partition.RecordImpl{
			ID:        "2",
			Timestamp: time.Now(),
			Data:      []byte("second"),
		},
		&partition.RecordImpl{
			ID:        "3",
			Timestamp: time.Now(),
			Data:      []byte("third"),
		},
	}

	// Write records to a buffer
	var buf bytes.Buffer
	for _, record := range records {
		_, err := recordio.Write(&buf, record)
		if err != nil {
			fmt.Printf("Error writing record: %v\n", err)
			return
		}
	}

	// Read records back using iterator
	for record := range recordio.Seq(&buf) {
		fmt.Printf("Read record: %s = %s\n",
			record.GetID(), record.GetData())
	}

	// Output:
	// Read record: 1 = first
	// Read record: 2 = second
	// Read record: 3 = third
}

// ExampleSize demonstrates calculating record sizes.
func ExampleSize() {
	record := &partition.RecordImpl{
		ID:           "test1",
		PartitionKey: "partition1",
		Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Data:         []byte("Hello, World!"),
	}

	size := recordio.Size(record)
	fmt.Printf("Record will occupy %d bytes\n", size)

	// Write record to verify size calculation
	var buf bytes.Buffer
	n, err := recordio.Write(&buf, record)
	if err != nil {
		fmt.Printf("Error writing record: %v\n", err)
		return
	}

	fmt.Printf("Actually wrote %d bytes\n", n)

	// Output:
	// Record will occupy 74 bytes
	// Actually wrote 74 bytes
}
