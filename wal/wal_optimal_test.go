package wal_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/davidvella/xp/wal"
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
			wantErr:   wal.ErrInvalidMaxRecords,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup
			tmpFile := createTempFile(t)
			defer os.Remove(tmpFile)

			// Create WAL
			wal, err := wal.Open(tmpFile, tt.maxRecs)
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
		setup   func(*wal.WAL) error
		wantErr error
	}{
		{
			name: "close empty WAL",
			setup: func(w *wal.WAL) error {
				return nil
			},
			wantErr: nil,
		},
		{
			name: "close WAL with unflushed records",
			setup: func(w *wal.WAL) error {
				return w.Write(createRecord("1", "part1", time.Now(), []byte("data1")))
			},
			wantErr: nil,
		},
		{
			name: "double close",
			setup: func(w *wal.WAL) error {
				return w.Close()
			},
			wantErr: wal.ErrWALClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := createTempFile(t)
			defer os.Remove(tmpFile)

			w, err := wal.Open(tmpFile, 10)
			require.NoError(t, err)

			err = tt.setup(w)
			require.NoError(t, err)

			err = w.Close()
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
		wal, err := wal.Open(tmpFile, 10)
		require.NoError(t, err)
		defer wal.Close()

		for _, record := range records {
			err := wal.Write(record)
			require.NoError(t, err)
		}
	}()

	// Reopen WAL and verify records
	func() {
		wal, err := wal.Open(tmpFile, 10)
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

func createTestRecord(id string, data []byte) partition.Record {
	return partition.RecordImpl{
		ID:           id,
		PartitionKey: "test-partition",
		Timestamp:    time.Now(),
		Data:         data,
	}
}

func BenchmarkWALWriter(b *testing.B) {
	benchCases := []struct {
		name      string
		records   int
		dataSize  int
		batchSize int
	}{
		{
			name:      "SmallRecords",
			records:   1000,
			dataSize:  100,
			batchSize: 100,
		},
		{
			name:      "MediumRecords",
			records:   10000,
			dataSize:  1000,
			batchSize: 1000,
		},
		{
			name:      "LargeRecords",
			records:   100000,
			dataSize:  10000,
			batchSize: 10000,
		},
	}

	for _, bc := range benchCases {
		// Generate test data
		testData := make([]partition.Record, bc.records)
		data := make([]byte, bc.dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		for i := range testData {
			testData[i] = createTestRecord(
				fmt.Sprintf("id-%d", i),
				data,
			)
		}

		b.Run(fmt.Sprintf("%v", bc), func(b *testing.B) {
			tmpFile := b.TempDir() + "/wal.db"

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				// Create new WAL for each iteration
				w, err := wal.Open(tmpFile, bc.batchSize)
				require.NoError(b, err)
				b.StartTimer()

				for _, record := range testData {
					err := w.Write(record)
					require.NoError(b, err)
				}

				b.StopTimer()
				require.NoError(b, w.Close())
				os.Remove(tmpFile)
				b.StartTimer()
			}
		})

		b.Run(fmt.Sprintf("%v_Parallel", bc), func(b *testing.B) {
			tmpFile := b.TempDir() + "/wal.db"
			w, err := wal.Open(tmpFile, bc.batchSize)
			require.NoError(b, err)
			defer func() {
				w.Close()
				os.Remove(tmpFile)
			}()

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					// Each goroutine writes all records
					for _, record := range testData {
						err := w.Write(record)
						require.NoError(b, err)
					}
				}
			})
		})
	}
}

func BenchmarkWALWrite(b *testing.B) {
	benchCases := []struct {
		name     string
		dataSize int
	}{
		{
			name:     "SmallRecord",
			dataSize: 100,
		},
		{
			name:     "MediumRecord",
			dataSize: 1000,
		},
		{
			name:     "LargeRecord",
			dataSize: 10000,
		},
	}

	for _, bc := range benchCases {
		b.Run(fmt.Sprintf("%v", bc), func(b *testing.B) {
			// Create temporary file for WAL
			tmpFile := b.TempDir() + "/wal.db"

			// Generate test data
			data := make([]byte, bc.dataSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Create test record
			record := partition.RecordImpl{
				ID:           "test-id",
				PartitionKey: "test-partition",
				Timestamp:    time.Now(),
				Data:         data,
			}

			// Create new WAL for each iteration
			w, err := wal.Open(tmpFile, 1000)
			require.NoError(b, err)
			defer func() {
				w.Close()
				os.Remove(tmpFile)
			}()

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				err := w.Write(record)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		// Test parallel writes
		b.Run(fmt.Sprintf("%v", bc)+"_Parallel", func(b *testing.B) {
			tmpFile := b.TempDir() + "/wal.db"

			data := make([]byte, bc.dataSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			record := partition.RecordImpl{
				ID:           "test-id",
				PartitionKey: "test-partition",
				Timestamp:    time.Now(),
				Data:         data,
			}

			w, err := wal.Open(tmpFile, 1000)
			require.NoError(b, err)
			defer func() {
				w.Close()
				os.Remove(tmpFile)
			}()

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					err := w.Write(record)
					if err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}
