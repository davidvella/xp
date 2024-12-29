package wal

import (
	"os"
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWAL(t *testing.T) {
	tests := []struct {
		name      string
		records   []partition.Record
		maxRecs   int
		wantErr   error
		wantCount int
	}{
		{
			name: "basic write and read",
			records: []partition.Record{
				createRecord("1", "part1", time.Now(), []byte("data1")),
				createRecord("2", "part1", time.Now(), []byte("data2")),
			},
			maxRecs:   10,
			wantErr:   nil,
			wantCount: 2,
		},
		{
			name: "segment rotation",
			records: []partition.Record{
				createRecord("1", "part1", time.Now(), []byte("data1")),
				createRecord("2", "part1", time.Now(), []byte("data2")),
				createRecord("3", "part1", time.Now(), []byte("data3")),
			},
			maxRecs:   2,
			wantErr:   nil,
			wantCount: 3,
		},
		{
			name:      "invalid max records",
			records:   []partition.Record{},
			maxRecs:   0,
			wantErr:   ErrInvalidMaxRecords,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			tmpFile := createTempFile(t)
			defer os.Remove(tmpFile)

			// Create WAL
			wal, err := NewWAL(tmpFile, tt.maxRecs)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			defer wal.Close()

			// Write records
			for _, record := range tt.records {
				err := wal.Write(record)
				require.NoError(t, err)
			}

			// Read all records
			var readRecords []partition.Record
			for record := range wal.ReadAll() {
				readRecords = append(readRecords, record)
			}

			// Assertions
			assert.Equal(t, tt.wantCount, len(readRecords))

			// Verify records are in order
			if len(readRecords) > 1 {
				for i := 0; i < len(readRecords)-1; i++ {
					assert.False(t, readRecords[i+1].Less(readRecords[i]),
						"Records should be in ascending order")
				}
			}
		})
	}
}

func TestWALClose(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*WAL) error
		wantErr error
	}{
		{
			name: "close empty WAL",
			setup: func(w *WAL) error {
				return nil
			},
			wantErr: nil,
		},
		{
			name: "close WAL with unflushed records",
			setup: func(w *WAL) error {
				return w.Write(createRecord("1", "part1", time.Now(), []byte("data1")))
			},
			wantErr: nil,
		},
		{
			name: "double close",
			setup: func(w *WAL) error {
				return w.Close()
			},
			wantErr: ErrWALClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := createTempFile(t)
			defer os.Remove(tmpFile)

			wal, err := NewWAL(tmpFile, 10)
			require.NoError(t, err)

			err = tt.setup(wal)
			require.NoError(t, err)

			err = wal.Close()
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestWALPersistence(t *testing.T) {
	// Create temp file
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	// Test data
	records := []partition.Record{
		createRecord("1", "part1", time.Now(), []byte("data1")),
		createRecord("2", "part1", time.Now(), []byte("data2")),
		createRecord("3", "part2", time.Now(), []byte("data3")),
	}

	// Write records to WAL
	func() {
		wal, err := NewWAL(tmpFile, 10)
		require.NoError(t, err)
		defer wal.Close()

		for _, record := range records {
			err := wal.Write(record)
			require.NoError(t, err)
		}
	}()

	// Reopen WAL and verify records
	func() {
		wal, err := NewWAL(tmpFile, 10)
		require.NoError(t, err)
		defer wal.Close()

		// Read all records
		var readRecords []partition.Record
		for record := range wal.ReadAll() {
			readRecords = append(readRecords, record)
		}

		// Verify record count
		assert.Equal(t, len(records), len(readRecords))

		// Verify record contents
		for i, expected := range records {
			actual := readRecords[i]
			assert.Equal(t, expected.GetID(), actual.GetID())
			assert.Equal(t, expected.GetPartitionKey(), actual.GetPartitionKey())
			assert.Equal(t, expected.GetData(), actual.GetData())
			// Note: Timestamp comparison might need tolerance depending on serialization precision
		}

		// Verify records are in order
		for i := 0; i < len(readRecords)-1; i++ {
			assert.False(t, readRecords[i+1].Less(readRecords[i]),
				"Records should be in ascending order")
		}
	}()
}

// Helper functions
func createTempFile(t *testing.T) string {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "wal_test_*.db")
	require.NoError(t, err)
	tmpFile.Close()
	return tmpFile.Name()
}

func createRecord(id, partKey string, ts time.Time, data []byte) partition.Record {
	return partition.RecordImpl{
		ID:           id,
		PartitionKey: partKey,
		Timestamp:    ts,
		Data:         data,
	}
}
