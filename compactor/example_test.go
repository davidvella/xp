package compactor_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/davidvella/xp/compactor"
	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/sstable"
)

type WriteSeeker struct {
	*bytes.Buffer
	pos int64
}

func (ws *WriteSeeker) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = ws.pos + offset
	case io.SeekEnd:
		abs = int64(ws.Len()) + offset
	default:
		return 0, errors.New("invalid whence")
	}

	if abs < 0 {
		return 0, errors.New("negative position")
	}

	ws.pos = abs
	return abs, nil
}

func (ws *WriteSeeker) Write(p []byte) (n int, err error) {
	n, err = ws.Buffer.Write(p)
	ws.pos += int64(n)
	return n, err
}

// ExampleCompact demonstrates basic compaction of multiple sequences.
func ExampleCompact() {
	// Create sequences with overlapping records
	seq1 := NewList[partition.Record](
		&partition.RecordImpl{
			ID:        "1",
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Data:      []byte("version1"),
		},
		&partition.RecordImpl{
			ID:        "2",
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Data:      []byte("record2"),
		},
	)

	seq2 := NewList[partition.Record](
		&partition.RecordImpl{
			ID:        "1",
			Timestamp: time.Date(2024, 1, 1, 0, 0, 1, 0, time.UTC),
			Data:      []byte("version2"),
		},
		&partition.RecordImpl{
			ID:        "3",
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			Data:      []byte("record3"),
		},
	)

	// Create a buffer to hold the SSTable
	var buf = WriteSeeker{Buffer: bytes.NewBuffer(nil)}

	// Compact the sequences
	err := compactor.Compact(&buf, seq1, seq2)
	if err != nil {
		fmt.Printf("Error during compaction: %v\n", err)
		return
	}

	// Read back the compacted records
	reader, err := sstable.OpenReader(bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		fmt.Printf("Error opening reader: %v\n", err)
		return
	}
	defer reader.Close()

	// Print all records
	for record := range reader.All() {
		fmt.Printf("ID: %s, Data: %s\n", record.GetID(), record.GetData())
	}

	// Output:
	// ID: 1, Data: version2
	// ID: 2, Data: record2
	// ID: 3, Data: record3
}

// ExampleCompact_empty demonstrates handling empty sequences.
func ExampleCompact_empty() {
	// Create an empty sequence
	emptySeq := NewList[partition.Record]()

	// Create a buffer to hold the SSTable
	var buf = WriteSeeker{Buffer: bytes.NewBuffer(nil)}

	// Compact the empty sequence
	err := compactor.Compact(&buf, emptySeq)
	if err != nil {
		fmt.Printf("Error during compaction: %v\n", err)
		return
	}

	// Read back the compacted records
	reader, err := sstable.OpenReader(bytes.NewReader(buf.Bytes()), nil)
	if err != nil {
		fmt.Printf("Error opening reader: %v\n", err)
		return
	}
	defer reader.Close()

	// Print record count
	count := 0
	for range reader.All() {
		count++
	}
	fmt.Printf("Number of records: %d\n", count)

	// Output:
	// Number of records: 0
}
