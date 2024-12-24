package composite

import "github.com/davidvella/xp/core/partition"

var _ partition.Strategy = &Strategy{}

type Strategy struct {
	partitions []partition.Strategy
}

func NewStrategy(partitions ...partition.Strategy) *Strategy {
	return &Strategy{partitions: partitions}
}

func (c *Strategy) ShouldRotate(current, incoming partition.Record) bool {
	for _, p := range c.partitions {
		if rotate := p.ShouldRotate(current, incoming); rotate {
			return rotate
		}
	}
	return false
}
