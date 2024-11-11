package serialization

import (
	"testing"
	"time"

	"github.com/davidvella/xp/core/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGobSerializer(t *testing.T) {
	type TestEvent struct {
		ID     string
		Value  float64
		Labels map[string]string
	}

	serializer := NewGobSerializer[TestEvent]()

	t.Run("Single Value", func(t *testing.T) {
		original := TestEvent{
			ID:    "test",
			Value: 123.45,
			Labels: map[string]string{
				"key": "value",
			},
		}

		bytes, err := serializer.SerializeValue(original)
		require.NoError(t, err)

		decoded, err := serializer.DeserializeValue(bytes)
		require.NoError(t, err)

		assert.Equal(t, original, decoded)
	})

	t.Run("Record Chunk", func(t *testing.T) {
		original := []types.Record[TestEvent]{
			{
				Data: TestEvent{
					ID:     "1",
					Value:  100,
					Labels: map[string]string{"type": "A"},
				},
				Timestamp: time.Now().Truncate(time.Second),
			},
			{
				Data: TestEvent{
					ID:     "2",
					Value:  200,
					Labels: map[string]string{"type": "B"},
				},
				Timestamp: time.Now().Truncate(time.Second),
			},
		}

		bytes, err := serializer.SerializeChunk(original)
		require.NoError(t, err)

		decoded, err := serializer.DeserializeChunk(bytes)
		require.NoError(t, err)

		assert.Equal(t, len(original), len(decoded))
		for i := range original {
			assert.Equal(t, original[i], decoded[i])
		}
	})

	t.Run("Empty Chunk", func(t *testing.T) {
		original := []types.Record[TestEvent]{}

		bytes, err := serializer.SerializeChunk(original)
		require.NoError(t, err)

		decoded, err := serializer.DeserializeChunk(bytes)
		require.NoError(t, err)

		assert.Empty(t, decoded)
	})
}
