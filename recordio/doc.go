// Package recordio implements a binary record format for storing and retrieving
// partition.Record instances. It provides efficient serialization and deserialization
// with magic bytes for format validation and length-prefixed fields for reliable
// parsing.
//
// Basic usage:
//
//	// Writing a record
//	record := partition.RecordImpl{
//	    ID:           "record1",
//	    PartitionKey: "partition1",
//	    Timestamp:    time.Now(),
//	    Data:         []byte("Hello, World!"),
//	}
//
//	var buf bytes.Buffer
//	n, err := recordio.Write(&buf, &record)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Reading records
//	for record := range recordio.Seq(&buf) {
//	    fmt.Printf("Read record: %s\n", record.GetID())
//	}
//
//	// Calculate record size
//	size := recordio.Size(&record)
package recordio
