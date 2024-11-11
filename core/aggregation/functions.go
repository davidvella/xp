package aggregation

import "github.com/davidvella/xp/core/types"

// AggregateFunction defines how grouped records should be aggregated
type AggregateFunction[K types.GroupKey, T any, R any] interface {
	// CreateAccumulator creates a new accumulator for aggregation
	CreateAccumulator() R
	// AddInput adds a single input to the accumulator
	AddInput(acc R, input types.Record[T]) R
	// MergeAccumulators merges multiple accumulators
	MergeAccumulators(acc1, acc2 R) R
	// GetResult produces the final result from the accumulator
	GetResult(acc R) R
}

// CountAggregator counts records in each group
type CountAggregator[K types.GroupKey, T any] struct{}

func (a *CountAggregator[K, T]) CreateAccumulator() int64 {
	return 0
}

func (a *CountAggregator[K, T]) AddInput(acc int64, input types.Record[T]) int64 {
	return acc + 1
}

func (a *CountAggregator[K, T]) MergeAccumulators(acc1, acc2 int64) int64 {
	return acc1 + acc2
}

func (a *CountAggregator[K, T]) GetResult(acc int64) int64 {
	return acc
}

// SumAggregator sums numeric values
type SumAggregator[K types.GroupKey, T any] struct {
	valueFn func(T) float64
}

func NewSumAggregator[K types.GroupKey, T any](valueFn func(T) float64) *SumAggregator[K, T] {
	return &SumAggregator[K, T]{valueFn: valueFn}
}

func (a *SumAggregator[K, T]) CreateAccumulator() float64 {
	return 0
}

func (a *SumAggregator[K, T]) AddInput(acc float64, input types.Record[T]) float64 {
	return acc + a.valueFn(input.Data)
}

func (a *SumAggregator[K, T]) MergeAccumulators(acc1, acc2 float64) float64 {
	return acc1 + acc2
}

func (a *SumAggregator[K, T]) GetResult(acc float64) float64 {
	return acc
}
