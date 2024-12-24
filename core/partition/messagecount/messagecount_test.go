package messagecount

import (
	"testing"

	"github.com/davidvella/xp/core/partition"
	"github.com/stretchr/testify/assert"
)

func TestStrategy_ShouldRotate(t *testing.T) {
	tests := []struct {
		name        string
		maxMessages int
		current     partition.Record
		incoming    partition.Record
		presetCount int
		want        bool
	}{
		{
			name:        "under max messages returns false",
			maxMessages: 10,
			current:     partition.RecordImpl{PartitionKey: "key1"},
			incoming:    partition.RecordImpl{PartitionKey: "key1"},
			presetCount: 5,
			want:        false,
		},
		{
			name:        "at max messages returns true",
			maxMessages: 10,
			current:     partition.RecordImpl{PartitionKey: "key1"},
			incoming:    partition.RecordImpl{PartitionKey: "key1"},
			presetCount: 9,
			want:        true,
		},
		{
			name:        "over max messages returns true",
			maxMessages: 10,
			current:     partition.RecordImpl{PartitionKey: "key1"},
			incoming:    partition.RecordImpl{PartitionKey: "key1"},
			presetCount: 15,
			want:        true,
		},
		{
			name:        "different keys tracked separately",
			maxMessages: 10,
			current:     partition.RecordImpl{PartitionKey: "key1"},
			incoming:    partition.RecordImpl{PartitionKey: "key2"},
			presetCount: 9,
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New(tt.maxMessages)
			if tt.presetCount > 0 {
				s.counter[tt.current.GetPartitionKey()] = tt.presetCount
			}
			got := s.ShouldRotate(tt.current, tt.incoming)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestStrategy_ConcurrentAccess(t *testing.T) {
	s := New(10)

	// Test concurrent access to GetPartitionPath and ShouldRotate
	done := make(chan bool)

	go func() {
		for i := 0; i < 100; i++ {
			current := partition.RecordImpl{PartitionKey: "key1"}
			incoming := partition.RecordImpl{PartitionKey: "key1"}
			s.ShouldRotate(current, incoming)
		}
		done <- true
	}()

	// Wait for both goroutines to complete
	<-done
}
