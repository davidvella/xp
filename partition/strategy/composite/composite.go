package composite

import (
	"time"

	"github.com/davidvella/xp/core/partition"
)

var _ partition.Strategy = &Strategy{}

type Strategy struct {
	partitions []partition.Strategy
}

func NewStrategy(partitions ...partition.Strategy) *Strategy {
	return &Strategy{partitions: partitions}
}

func (c *Strategy) ShouldRotate(information partition.Information, t time.Time) bool {
	for _, p := range c.partitions {
		if rotate := p.ShouldRotate(information, t); rotate {
			return rotate
		}
	}
	return false
}
