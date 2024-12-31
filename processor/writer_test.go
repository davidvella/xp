package processor

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWalKey(t *testing.T) {
	tests := []struct {
		name        string
		writer      WalKey
		serialized  string
		shouldError bool
	}{
		{
			name: "valid writer",
			writer: WalKey{
				PartitionKey: "partition1",
				Watermark:    time.Unix(1643673600, 0), // 2022-02-01
			},
			serialized:  "partition1_1643673600.wal",
			shouldError: false,
		},
		{
			name: "special characters in partition",
			writer: WalKey{
				PartitionKey: "part-123_test",
				Watermark:    time.Unix(1643673600, 0),
			},
			serialized:  "part-123_test_1643673600.wal",
			shouldError: true, // Deserializing will fail due to multiple underscores
		},
		{
			name:        "invalid format",
			serialized:  "invalid.wal",
			shouldError: true,
		},
		{
			name:        "invalid timestamp",
			serialized:  "partition_abc.wal",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.writer != (WalKey{}) {
				result := Serialize(tt.writer)
				assert.Equal(t, tt.serialized, result)
			}

			writer, err := Deserialize(tt.serialized)
			if tt.shouldError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.writer.PartitionKey, writer.PartitionKey)
				assert.Equal(t, tt.writer.Watermark.Unix(), writer.Watermark.Unix())
			}
		})
	}
}
