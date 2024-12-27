package messagecount

import (
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
	"github.com/stretchr/testify/assert"
)

func TestStrategy_ShouldRotate(t *testing.T) {
	tests := []struct {
		name        string
		strategy    *Strategy
		information partition.Information
		currentTime time.Time
		want        bool
	}{
		{
			name:        "should rotate when record count equals max messages",
			strategy:    NewStrategy(100),
			information: partition.Information{RecordCount: 100},
			currentTime: time.Now(),
			want:        true,
		},
		{
			name:        "should rotate when record count exceeds max messages",
			strategy:    NewStrategy(100),
			information: partition.Information{RecordCount: 150},
			currentTime: time.Now(),
			want:        true,
		},
		{
			name:        "should not rotate when record count below max messages",
			strategy:    NewStrategy(100),
			information: partition.Information{RecordCount: 50},
			currentTime: time.Now(),
			want:        false,
		},
		{
			name:        "should not rotate with zero records",
			strategy:    NewStrategy(100),
			information: partition.Information{RecordCount: 0},
			currentTime: time.Now(),
			want:        false,
		},
		{
			name:        "should handle zero max messages",
			strategy:    NewStrategy(0),
			information: partition.Information{RecordCount: 1},
			currentTime: time.Now(),
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.strategy.ShouldRotate(tt.information, tt.currentTime)
			assert.Equal(t, tt.want, got)
		})
	}
}
