package composite

import "github.com/davidvella/xp/core/partition"

var _ partition.Strategy = &Partition{}

type Partition struct {
	partitions []partition.Strategy
}

func NewPartition(partitions ...partition.Strategy) *Partition {
	return &Partition{partitions: partitions}
}

func (c *Partition) ShouldRotate(current, incoming partition.Record) bool {
	for _, p := range c.partitions {
		if rotate := p.ShouldRotate(current, incoming); rotate {
			return rotate
		}
	}
	return false
}
