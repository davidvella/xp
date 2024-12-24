package timewindow

import (
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/stretchr/testify/assert"
)

func TestTimeWindowStrategy(t *testing.T) {
	tests := []struct {
		name         string
		windowSize   time.Duration
		current      partition.Record
		incoming     partition.Record
		expectRotate bool
	}{
		{
			name:       "same window - no rotation",
			windowSize: 5 * time.Minute,
			current: partition.Record{
				Timestamp: time.Unix(1000, 0),
			},
			incoming: partition.Record{
				Timestamp: time.Unix(1001, 0),
			},
			expectRotate: false,
		},
		{
			name:       "different window - should rotate",
			windowSize: 5 * time.Minute,
			current: partition.Record{
				Timestamp: time.Unix(1000, 0),
			},
			incoming: partition.Record{
				Timestamp: time.Unix(1301, 0),
			},
			expectRotate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := New(tt.windowSize)

			rotate := strategy.ShouldRotate(tt.current, tt.incoming)
			assert.Equal(t, tt.expectRotate, rotate)
		})
	}
}
