// Package wal implements a Write-Ahead Log (WAL) for durable and atomic record storage.
//
// A Write-Ahead Log is a reliability mechanism that ensures data persistence by writing
// records sequentially to disk before they are considered committed. The WAL is organized
// into segments, where each segment contains a sorted collection of records.
//
// The WAL implementation provides the following guarantees:
//   - Durability: Records are persisted to disk before operations are considered complete
//   - Atomicity: Records within a segment are written atomically
//   - Order preservation: Records are maintained in sorted order within segments
//
// Basic usage:
//
//	file, err := os.Create("mywal.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Create a new WAL writer with segments of up to 1000 records
//	writer, err := wal.NewWriter(file, 1000)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Write records
//	err = writer.Write(myRecord)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Close the writer
//	err = writer.Close()
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	// Read records back
//	file, err = os.Open("mywal.db")
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	reader := wal.NewReader(file)
//	for record := range reader.All() {
//	    // Process record
//	}
//
// File Format:
// Each segment in the WAL consists of:
//   - Segment header: Total size of the segment (8 bytes)
//   - Records: Series of variable-length records
//
// The WAL automatically rotates segments when they reach the configured maximum
// number of records, ensuring efficient storage and retrieval.
package wal
