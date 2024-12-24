package loser_test

import (
	"iter"
	"math"
	"testing"

	"github.com/davidvella/xp/core/loser"
)

type List[E loser.Lesser[E]] struct {
	list []E
}

func NewList[E loser.Lesser[E]](list ...E) *List[E] {
	return &List[E]{list: list}
}

func (it *List[E]) All() iter.Seq[E] {
	return func(yield func(E) bool) {
		for _, i := range it.list {
			yield(i)
		}
	}
}

func checkIterablesEqual[E loser.Lesser[E]](t *testing.T, a loser.Sequence[E], b loser.Sequence[E]) {
	t.Helper()
	count := 0
	next, stop := iter.Pull(b.All())
	defer stop()
	for va := range a.All() {
		count++
		vb, ok := next()
		if !ok {
			t.Fatalf("b ended before a after %d elements", count)
		}
		if va.Less(vb) || vb.Less(va) {
			t.Fatalf("position %d: %v != %v", count, va, vb)
		}
	}
	if _, ok := next(); ok {
		t.Fatalf("a ended before b after %d elements", count)
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name string
		args []loser.Sequence[Uint64]
		want *List[Uint64]
	}{
		{
			name: "empty input",
			want: NewList[Uint64](),
		},
		{
			name: "one list",
			args: []loser.Sequence[Uint64]{NewList[Uint64](1, 2, 3, 4)},
			want: NewList[Uint64](1, 2, 3, 4),
		},
		{
			name: "two lists",
			args: []loser.Sequence[Uint64]{NewList[Uint64](3, 4, 5), NewList[Uint64](1, 2)},
			want: NewList[Uint64](1, 2, 3, 4, 5),
		},
		{
			name: "two lists, first empty",
			args: []loser.Sequence[Uint64]{NewList[Uint64](), NewList[Uint64](1, 2)},
			want: NewList[Uint64](1, 2),
		},
		{
			name: "two lists, second empty",
			args: []loser.Sequence[Uint64]{NewList[Uint64](1, 2), NewList[Uint64]()},
			want: NewList[Uint64](1, 2),
		},
		{
			name: "two lists b",
			args: []loser.Sequence[Uint64]{NewList[Uint64](1, 2), NewList[Uint64](3, 4, 5)},
			want: NewList[Uint64](1, 2, 3, 4, 5),
		},
		{
			name: "two lists c",
			args: []loser.Sequence[Uint64]{NewList[Uint64](1, 3), NewList[Uint64](2, 4, 5)},
			want: NewList[Uint64](1, 2, 3, 4, 5),
		},
		{
			name: "three lists",
			args: []loser.Sequence[Uint64]{NewList[Uint64](1, 3), NewList[Uint64](2, 4), NewList[Uint64](5)},
			want: NewList[Uint64](1, 2, 3, 4, 5),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lt := loser.New[Uint64](tt.args, math.MaxUint64)
			checkIterablesEqual(t, tt.want, lt)
		})
	}
}

type Uint64 uint64

func (u Uint64) Less(other Uint64) bool {
	return u < other
}
