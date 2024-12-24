package recordio

import (
	"bytes"
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/stretchr/testify/assert"
)

func TestWrite(t *testing.T) {
	tests := []struct {
		name    string
		record  partition.Record
		wantErr bool
	}{
		{
			name: "successful write",
			record: partition.Record{
				Data:      []byte("test data"),
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
		{
			name: "empty data write",
			record: partition.Record{
				Data:      []byte{},
				Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := new(bytes.Buffer)
			err := Write(buf, tt.record)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)

			// Verify the written data
			records := ReadRecords(bytes.NewReader(buf.Bytes()))
			assert.Len(t, records, 1)

			// Compare the original and read record
			assert.Equal(t, tt.record.Data, records[0].Data)
			assert.Equal(t, tt.record.Timestamp, records[0].Timestamp)
		})
	}
}

func TestReadRecords(t *testing.T) {
	tests := []struct {
		name  string
		input func() *bytes.Buffer
		want  []partition.Record
	}{
		{
			name: "read single record",
			input: func() *bytes.Buffer {
				buf := new(bytes.Buffer)
				record := partition.Record{
					Data:      []byte("test data"),
					Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				}
				Write(buf, record)
				return buf
			},
			want: []partition.Record{
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
				records := []partition.Record{
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
					Write(buf, r)
				}
				return buf
			},
			want: []partition.Record{
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
			want: []partition.Record{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ReadRecords(tt.input())

			assert.Len(t, got, len(tt.want))

			for i := range got {
				assert.Equal(t, tt.want[i].Data, got[i].Data)
				assert.Equal(t, tt.want[i].Timestamp.UnixNano(), got[i].Timestamp.UnixNano())
			}
		})
	}
}