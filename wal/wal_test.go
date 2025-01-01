package wal_test

import (
	"fmt"
	"os"
	"strings"
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
			defer os.Remove(tmpFile.Name())

			// Create WAL
			writer, err := wal.NewWriter(tmpFile, tt.maxRecs)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			// Write records
			for _, record := range tt.records {
				err := writer.Write(record)
				require.NoError(t, err)
			}

			require.NoError(t, writer.Close())

			f, err := os.Open(tmpFile.Name())
			require.NoError(t, err)

			reader := wal.NewReader(f)
			// Read all records
			var readRecords []partition.Record
			for record := range reader.All() {
				readRecords = append(readRecords, record)
			}

			// Assertions
			assert.Equal(t, tt.wantCount, len(readRecords))

			// Verify records are in order
			if len(readRecords) > 1 {
				for i := 0; i < len(readRecords)-1; i++ {
					assert.False(t, partition.Less(readRecords[i+1], readRecords[i]),
						"Records should be in ascending order")
				}
			}
		})
	}
}

func TestWALClose(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(writer *wal.Writer) error
		wantErr error
	}{
		{
			name: "close empty WAL",
			setup: func(_ *wal.Writer) error {
				return nil
			},
			wantErr: nil,
		},
		{
			name: "close WAL with unflushed records",
			setup: func(w *wal.Writer) error {
				return w.Write(createRecord("1", "part1", time.Now(), []byte("data1")))
			},
			wantErr: nil,
		},
		{
			name: "double close",
			setup: func(w *wal.Writer) error {
				return w.Close()
			},
			wantErr: wal.ErrWALClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpFile := createTempFile(t)
			defer os.Remove(tmpFile.Name())

			w, err := wal.NewWriter(tmpFile, 10)
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
	defer os.Remove(tmpFile.Name())

	// Test data
	records := []partition.Record{
		createRecord("1", "part1", time.Now(), []byte("data1")),
		createRecord("2", "part1", time.Now(), []byte("data2")),
		createRecord("3", "part2", time.Now(), []byte("data3")),
	}

	// Write records to WAL
	func() {
		writer, err := wal.NewWriter(tmpFile, 10)
		require.NoError(t, err)
		defer writer.Close()

		for _, record := range records {
			err := writer.Write(record)
			require.NoError(t, err)
		}
	}()

	// Reopen WAL and verify records
	func() {
		f, err := os.Open(tmpFile.Name())
		require.NoError(t, err)
		defer f.Close()

		reader := wal.NewReader(f)

		// Read all records
		var readRecords []partition.Record
		for record := range reader.All() {
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
			assert.False(t, partition.Less(readRecords[i+1], readRecords[i]),
				"Records should be in ascending order")
		}
	}()
}

func createTempFile(t *testing.T) *os.File {
	t.Helper()
	tmpFile, err := os.CreateTemp(t.TempDir(), "wal_test_*.db")
	require.NoError(t, err)
	return tmpFile
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
		Timestamp:    time.Unix(0, 0),
		Data:         data,
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
			// Generate test data
			data := make([]byte, bc.dataSize)
			for i := range data {
				data[i] = byte(i % 256)
			}

			// Create test record
			record := createTestRecord("test", data)

			f, cleanup := setupBenchmarkTable(b)
			defer cleanup()

			w, err := wal.NewWriter(f, 1000)
			require.NoError(b, err)

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

			f, cleanup := setupBenchmarkTable(b)
			defer cleanup()

			w, err := wal.NewWriter(f, 1000)
			require.NoError(b, err)

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

func setupBenchmarkTable(b *testing.B) (file *os.File, cleanup func()) {
	b.Helper()

	tmpFile, err := os.CreateTemp(b.TempDir(), "sstable-bench-*.sst")
	if err != nil {
		b.Fatal(err)
	}

	cleanup = func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}

	return tmpFile, cleanup
}

func TestWALWriteErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func() (*wal.Writer, func())
		wantErr error
	}{
		{
			name: "write to closed WAL",
			setup: func() (*wal.Writer, func()) {
				tmpFile := createTempFile(t)
				cleanup := func() { os.Remove(tmpFile.Name()) }

				writer, err := wal.NewWriter(tmpFile, 10)
				require.NoError(t, err)
				require.NoError(t, writer.Close())

				return writer, cleanup
			},
			wantErr: wal.ErrWALClosed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer, cleanup := tt.setup()
			defer cleanup()

			err := writer.Write(createTestRecord("test", []byte("data")))
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestWALLargeFileRead(t *testing.T) {
	// Create temp file
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile.Name())

	const (
		numRecords = 20000
		dataSize   = 1024 // 1KB per record
	)

	// Generate test data
	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Write many records
	func() {
		writer, err := wal.NewWriter(tmpFile, 1000)
		require.NoError(t, err)
		defer writer.Close()
		current := "a"
		for i := 0; i < numRecords; i++ {
			record := createTestRecord(fmt.Sprintf("test-%s", current), data)
			err := writer.Write(record)
			require.NoError(t, err)
			current = incrementString(current)
		}
	}()

	// Read and verify records
	func() {
		f, err := os.Open(tmpFile.Name())
		require.NoError(t, err)
		defer f.Close()

		reader := wal.NewReader(f)
		count := 0
		current := "a"
		for record := range reader.All() {
			// Verify record order
			require.Equal(t, fmt.Sprintf("test-%s", current), record.GetID())
			assert.Equal(t, "test-partition", record.GetPartitionKey())
			assert.Equal(t, dataSize, len(record.GetData()))
			current = incrementString(current)
			count++
		}

		require.Equal(t, numRecords, count, "Should read all records")
	}()
}

func incrementString(s string) string {
	i := len(s) - 1
	for i >= 0 && s[i] == 'z' {
		i--
	}

	if i == -1 {
		return s + "a"
	}

	j := 0
	return strings.Map(func(r rune) rune {
		if j == i {
			r++
		}
		j++
		return r
	}, s)
}

func FuzzWALOperations(f *testing.F) {
	f.Add([]byte("test-data"), int(1))
	f.Add([]byte{}, int(0))

	f.Fuzz(func(t *testing.T, data []byte, maxRecs int) {
		tmpFile := createTempFile(t)
		defer os.Remove(tmpFile.Name())

		writer, err := wal.NewWriter(tmpFile, maxRecs)
		if err != nil {
			return
		}

		record := partition.RecordImpl{
			ID:           "test-id",
			PartitionKey: "test-partition",
			Timestamp:    time.Now(),
			Data:         data,
		}

		if err := writer.Write(&record); err != nil {
			return
		}

		require.NoError(t, writer.Close())

		f, err := os.Open(tmpFile.Name())
		require.NoError(t, err)
		defer f.Close()

		reader := wal.NewReader(f)
		var readRecords []partition.Record
		for record := range reader.All() {
			readRecords = append(readRecords, record)
		}

		assert.Equal(t, 1, len(readRecords))
		assert.Equal(t, data, readRecords[0].GetData())
	})
}
