package recordio_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/davidvella/xp/core/recordio"
	"github.com/stretchr/testify/assert"
)

var errWrite = errors.New("i failed to write")

type mockWriter struct {
	errorCounter int
	counter      int
}

func (w *mockWriter) Write(p []byte) (n int, err error) {
	w.counter++
	if w.counter == w.errorCounter {
		return 0, errWrite
	}
	return len(p), nil
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name    string
		writer  io.Writer
		record  partition.Record
		wantErr bool
	}{
		{
			name: "successful write",
			record: partition.RecordImpl{
				Data:      []byte("test data"),
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "successful write all data",
			record: partition.RecordImpl{
				ID:           "ID",
				PartitionKey: "test",
				Data:         []byte("test data"),
				Timestamp:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "empty data write",
			record: partition.RecordImpl{
				Data:      []byte{},
				Timestamp: time.Unix(0, 0),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := recordio.Write(buf, tt.record)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// Verify the written data
			records := recordio.ReadRecords(bytes.NewReader(buf.Bytes()))
			assert.Len(t, records, 1)

			// Compare the original and read record
			assert.Equal(t, tt.record.GetData(), records[0].GetData())
			assert.Equal(t, tt.record.GetWatermark(), records[0].GetWatermark())
		})
	}
}

func TestWriteHandleError(t *testing.T) {
	tests := []struct {
		name               string
		writerCounterError int
		expectedError      string
	}{
		{
			name:               "error write 1",
			writerCounterError: 1,
			expectedError:      "error writing ID: error writing string length: i failed to write",
		},
		{
			name:               "error write 2",
			writerCounterError: 2,
			expectedError:      "error writing ID: error writing string content: i failed to write",
		},
		{
			name:               "error write 3",
			writerCounterError: 3,
			expectedError:      "error writing partition key: error writing string length: i failed to write",
		},
		{
			name:               "error write 4",
			writerCounterError: 4,
			expectedError:      "error writing partition key: error writing string content: i failed to write",
		},
		{
			name:               "error write 5",
			writerCounterError: 5,
			expectedError:      "error writing timestamp: i failed to write",
		},
		{
			name:               "error write 6",
			writerCounterError: 6,
			expectedError:      "error writing timezone: error writing string length: i failed to write",
		},
		{
			name:               "error write 7",
			writerCounterError: 7,
			expectedError:      "error writing timezone: error writing string content: i failed to write",
		},
		{
			name:               "error write 8",
			writerCounterError: 8,
			expectedError:      "error writing data: error writing bytes length: i failed to write",
		},
		{
			name:               "error write 9",
			writerCounterError: 9,
			expectedError:      "error writing data: error writing bytes content: i failed to write",
		},
		{
			name:               "error write 10",
			writerCounterError: 10,
			expectedError:      "error writing newline: i failed to write",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := mockWriter{
				errorCounter: tt.writerCounterError,
			}

			record := &partition.RecordImpl{
				Data:      []byte("test data"),
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			}

			err := recordio.Write(&writer, record)

			assert.EqualError(t, err, tt.expectedError)
		})
	}
}

var errRead = errors.New("i failed to read")

type mockReader struct {
	*bytes.Reader
	counter      int
	errorCounter int
}

func newMockReader(data []byte, errorCount int) *mockReader {
	return &mockReader{
		Reader:       bytes.NewReader(data),
		errorCounter: errorCount,
	}
}

func TestReadHandleError(t *testing.T) {
	tests := []struct {
		name             string
		readCounterError int
		expectedError    string
	}{
		{
			name:             "error read 1",
			readCounterError: 1,
			expectedError:    "error reading ID: error reading string length: i failed to read",
		},
		{
			name:             "error read 2",
			readCounterError: 2,
			expectedError:    "error reading partition key: error reading string length: i failed to read",
		},
		{
			name:             "error read 3",
			readCounterError: 3,
			expectedError:    "error reading timestamp: i failed to read",
		},
		{
			name:             "error read 4",
			readCounterError: 4,
			expectedError:    "error reading timezone: error reading string length: i failed to read",
		},
		{
			name:             "error read 5",
			readCounterError: 5,
			expectedError:    "error reading timezone: error reading string content: i failed to read",
		},
		{
			name:             "error read 6",
			readCounterError: 6,
			expectedError:    "error reading data: error reading bytes length: i failed to read",
		},
		{
			name:             "error read 7",
			readCounterError: 7,
			expectedError:    "error reading data: error reading bytes content: i failed to read",
		},
		{
			name:             "error read 8",
			readCounterError: 8,
			expectedError:    "error reading newline: i failed to read",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			record := &partition.RecordImpl{
				Data:      []byte("test data"),
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			}

			buf := new(bytes.Buffer)
			err := recordio.Write(buf, record)
			assert.NoError(t, err)

			reader := newMockReader(buf.Bytes(), tt.readCounterError)

			_, err = recordio.ReadRecord(reader)

			assert.EqualError(t, err, tt.expectedError)
		})
	}
}

func (r *mockReader) Read(p []byte) (n int, err error) {
	r.counter++
	if r.counter == r.errorCounter {
		return 0, errRead
	}
	return r.Reader.Read(p)
}

func TestNilRecords(t *testing.T) {
	buf := new(bytes.Buffer)
	err := recordio.Write(buf, nil)

	assert.NoError(t, err)

	// Verify the written data
	records := recordio.ReadRecords(bytes.NewReader(buf.Bytes()))
	assert.Len(t, records, 0)
}

func TestReadRecords(t *testing.T) {
	tests := []struct {
		name  string
		input func() *bytes.Buffer
		want  []partition.RecordImpl
	}{
		{
			name: "read single record",
			input: func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				record := partition.RecordImpl{
					Data:      []byte("test data"),
					Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				}
				err := recordio.Write(buf, record)
				assert.NoError(t, err)
				return buf
			},
			want: []partition.RecordImpl{
				{
					Data:      []byte("test data"),
					Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "read multiple records",
			input: func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				records := []partition.RecordImpl{
					{
						Data:      []byte("first"),
						Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
					},
					{
						Data:      []byte("second"),
						Timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
					},
				}
				for _, r := range records {
					err := recordio.Write(buf, r)
					assert.NoError(t, err)
				}
				return buf
			},
			want: []partition.RecordImpl{
				{
					Data:      []byte("first"),
					Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				},
				{
					Data:      []byte("second"),
					Timestamp: time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{
			name: "read empty input",
			input: func() *bytes.Buffer {
				return new(bytes.Buffer)
			},
			want: []partition.RecordImpl{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := recordio.ReadRecords(tt.input())

			assert.Len(t, got, len(tt.want))

			for i := range got {
				assert.Equal(t, tt.want[i].Data, got[i].GetData())
				assert.Equal(t, tt.want[i].Timestamp.UnixNano(), got[i].GetWatermark().UnixNano())
			}
		})
	}
}
