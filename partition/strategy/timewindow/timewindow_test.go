package timewindow

import (
	"testing"
	"time"

	"github.com/davidvella/xp/partition"
	"github.com/stretchr/testify/assert"
)

func TestTimeWindowStrategy(t *testing.T) {
	tests := []struct {
		name         string
		windowSize   time.Duration
		current      partition.Information
		incoming     time.Time
		expectRotate bool
	}{
		{
			name:       "same window - no rotation",
			windowSize: 5 * time.Minute,
			current: partition.Information{
				FirstWatermark: time.Unix(1000, 0),
			},
			incoming:     time.Unix(1001, 0),
			expectRotate: false,
		},
		{
			name:       "different window - should rotate",
			windowSize: 5 * time.Minute,
			current: partition.Information{
				FirstWatermark: time.Unix(1000, 0),
			},
			incoming:     time.Unix(1301, 0),
			expectRotate: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			strategy := NewStrategy(tt.windowSize)

			rotate := strategy.ShouldRotate(tt.current, tt.incoming)
			assert.Equal(t, tt.expectRotate, rotate)
		})
	}
}
