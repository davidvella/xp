package composite

import (
	"testing"
	"time"

	"github.com/davidvella/xp/core/partition"
)

// MockStrategy implements the partition.Strategy interface for testing.
type MockStrategy struct {
	shouldRotate bool
}

func (m MockStrategy) ShouldRotate(_ partition.Information, _ time.Time) bool {
	return m.shouldRotate
}

func TestPartition_ShouldRotate(t *testing.T) {
	info := partition.Information{}

	tests := []struct {
		name       string
		partitions []partition.Strategy
		want       bool
	}{
		{
			name:       "empty partitions returns false",
			partitions: []partition.Strategy{},
			want:       false,
		},
		{
			name: "single partition returns true",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: true},
			},
			want: true,
		},
		{
			name: "single partition returns false",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: false},
			},
			want: false,
		},
		{
			name: "multiple partitions, first true returns true",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: true},
				MockStrategy{shouldRotate: false},
			},
			want: true,
		},
		{
			name: "multiple partitions, second true returns true",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: false},
				MockStrategy{shouldRotate: true},
			},
			want: true,
		},
		{
			name: "multiple partitions all false returns false",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: false},
				MockStrategy{shouldRotate: false},
				MockStrategy{shouldRotate: false},
			},
			want: false,
		},
		{
			name: "multiple partitions all true returns true",
			partitions: []partition.Strategy{
				MockStrategy{shouldRotate: true},
				MockStrategy{shouldRotate: true},
				MockStrategy{shouldRotate: true},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewStrategy(tt.partitions...)
			if got := p.ShouldRotate(info, time.Now()); got != tt.want {
				t.Errorf("Strategy.ShouldRotate() = %v, want %v", got, tt.want)
			}
		})
	}
}
