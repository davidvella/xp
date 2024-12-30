package loser_test

import (
	"fmt"
	"math"

	"github.com/davidvella/xp/loser"
)

// ExampleNew_basic demonstrates basic usage of a loser tree to merge sorted sequences.
func ExampleNew_basic() {
	// Create three sorted sequences
	seq1 := NewList(1, 4, 7)
	seq2 := NewList(2, 5, 8)
	seq3 := NewList(3, 6, 9)

	// Create a loser tree to merge the sequences
	tree := loser.New(
		[]loser.Sequence[int]{seq1, seq2, seq3},
		math.MaxInt,
		func(a, b int) bool { return a < b },
	)

	// Print merged sequence
	for v := range tree.All() {
		fmt.Printf("%d ", v)
	}

	// Output: 1 2 3 4 5 6 7 8 9
}

// ExampleNew_strings shows how to use the loser tree with string sequences.
func ExampleNew_strings() {
	// Create sequences of sorted strings
	seq1 := NewList("apple", "dog", "zebra")
	seq2 := NewList("banana", "elephant")
	seq3 := NewList("cat", "fish")

	// Create loser tree for strings
	tree := loser.New(
		[]loser.Sequence[string]{seq1, seq2, seq3},
		"zzz", // Maximum possible string value
		func(a, b string) bool { return a < b },
	)

	// Print merged sequence
	for v := range tree.All() {
		fmt.Printf("%s ", v)
	}

	// Output: apple banana cat dog elephant fish zebra
}

// ExampleNew_empty demonstrates handling empty sequences.
func ExampleNew_empty() {
	// Create mix of empty and non-empty sequences
	seq1 := NewList(1, 3, 5)
	seq2 := NewList[int]() // Empty sequence
	seq3 := NewList(2, 4)

	tree := loser.New(
		[]loser.Sequence[int]{seq1, seq2, seq3},
		math.MaxInt,
		func(a, b int) bool { return a < b },
	)

	// Print merged sequence
	for v := range tree.All() {
		fmt.Printf("%d ", v)
	}

	// Output: 1 2 3 4 5
}
