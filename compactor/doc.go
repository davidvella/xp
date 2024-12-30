// Package compactor implements streaming compaction of multiple sorted sequences
// into a single SSTable. It uses a loser tree to efficiently merge sequences
// while deduplicating records based on their IDs, keeping only the most recent
// version of each record.
//
// The compaction process:
//   - Merges multiple sorted sequences into a single sorted sequence
//   - Deduplicates records with the same ID, keeping the latest version
//   - Writes the result to an SSTable for efficient storage and retrieval
//
// Key features:
//   - Streaming: Processes records one at a time without loading all data into memory
//   - Efficient: Uses a loser tree for optimal merging of multiple sequences
//   - Deduplication: Automatically handles multiple versions of the same record
//   - SSTable output: Results are written in the SSTable format for efficient access
//
// Basic usage:
//
//	// Create some sorted sequences of records
//	seq1 := NewList(
//	    partition.RecordImpl{ID: "1", Timestamp: time.Now(), Data: []byte("v1")},
//	    partition.RecordImpl{ID: "2", Timestamp: time.Now(), Data: []byte("v2")},
//	)
//	seq2 := NewList(
//	    partition.RecordImpl{ID: "1", Timestamp: time.Now(), Data: []byte("v3")},
//	    partition.RecordImpl{ID: "3", Timestamp: time.Now(), Data: []byte("v4")},
//	)
//
//	// Create output file
//	file, err := os.Create("output.sst")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer file.Close()
//
//	// Compact sequences into an SSTable
//	err = compactor.Compact(file, seq1, seq2)
//	if err != nil {
//	    log.Fatal(err)
//	}
//
// The compaction process ensures that:
//   - Records are processed in sorted order (by ID)
//   - When multiple records have the same ID, only the latest version is kept
//   - The output is written in SSTable format for efficient subsequent access
//   - Memory usage remains constant regardless of input size
//
// The package is particularly useful in database systems where multiple sorted
// sequences (e.g., from different SSTable files) need to be merged while
// maintaining data consistency and optimizing storage efficiency.
package compactor
