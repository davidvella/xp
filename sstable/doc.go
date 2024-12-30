// Package sstable implements a Sorted String Table (SSTable) for efficient storage and retrieval
// of key-value pairs. An SSTable is an immutable, ordered file format that maps keys to values,
// optimized for high-throughput sequential read/write operations.
//
// The SSTable format includes:
//   - A header with magic number and version
//   - A sequence of sorted key-value records
//   - A sparse index for efficient key lookups
//   - A footer containing the index offset and magic number
//
// Key features:
//   - Immutable: Once written, records cannot be modified
//   - Sorted: Records are stored in key order
//   - Efficient: Binary search enabled through sparse indexing
//   - Sequential: Optimized for sequential read/write patterns
//
// Basic usage:
//
//	// Writing records
//	writer, err := sstable.OpenWriter(file, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
//	record := partition.RecordImpl{
//	    ID:           "key1",
//	    PartitionKey: "partition1",
//	    Timestamp:    time.Now(),
//	    Data:         []byte("value1"),
//	}
//
//	if err := writer.Write(&record); err != nil {
//	    log.Fatal(err)
//	}
//	writer.Close()
//
//	// Reading records
//	reader, err := sstable.OpenReader(file, nil)
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer reader.Close()
//
//	// Get specific record
//	record, err := reader.Get("key1")
//
//	// Iterate all records
//	for record := range reader.All() {
//	    // Process record
//	}
//
// File Format:
//   - Header (16 bytes):
//   - Magic number (8 bytes, "SSTB" in hex)
//   - Format version (8 bytes)
//   - Records:
//   - Sequence of variable-length records in sorted order
//   - Sparse Index:
//   - Count of index entries (8 bytes)
//   - Sequence of (key, offset) pairs
//   - Footer (16 bytes):
//   - Index offset (8 bytes)
//   - Magic number (8 bytes, "ENDB" in hex)
//
// The sparse index enables efficient key lookups by maintaining a subset of keys
// and their file offsets, reducing the amount of data that needs to be read
// during binary search operations.
package sstable
