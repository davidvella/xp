// Package loser implements a tournament tree (also known as a loser tree) for efficiently
// merging multiple sorted sequences. This implementation is based on the work by Bryan
// Boreham (https://github.com/bboreham/go-loser).
//
// A loser tree is a binary tree structure where each internal node holds the "loser" of
// a comparison between its children, and the root holds the overall "winner". This makes
// it particularly efficient for merging multiple sorted sequences, as it requires fewer
// comparisons than a standard merge algorithm.
//
// Key features:
//   - Generic implementation supporting any comparable type
//   - Efficient merging of multiple sorted sequences
//   - Iterator-based interface using Go's iter.Seq
//   - O(log n) comparisons per element
//   - Memory-efficient implementation
//
// Basic usage:
//
//	// Create sorted sequences
//	seq1 := NewList(1, 3, 5)
//	seq2 := NewList(2, 4, 6)
//	seq3 := NewList(7, 8, 9)
//
//	// Create a loser tree to merge the sequences
//	tree := loser.New(
//	    []loser.Sequence[int]{seq1, seq2, seq3},
//	    math.MaxInt,  // Maximum possible value
//	    func(a, b int) bool { return a < b },
//	)
//
//	// Iterate over merged results
//	for v := range tree.All() {
//	    fmt.Println(v)  // Will print: 1, 2, 3, 4, 5, 6, 7, 8, 9
//	}
//
// Implementation Details:
// The loser tree is implemented as a binary tree laid out in an array where:
//   - For node N, its children are at positions 2N and 2N+1
//   - Leaf nodes are stored in positions M to 2M-1 (where M is the number of sequences)
//   - Internal nodes are stored in positions 1 to M-1
//   - Node 0 is special, containing the current winner
//
// Each node contains:
//   - index: The index of the losing sequence (or winning sequence for node 0)
//   - value: The value from the losing sequence
//   - next: Function to get next value (only for leaf nodes)
//
// The tree maintains the invariant that each internal node contains the larger
// (losing) value of its children, ensuring that the smallest (winning) value
// propagates to the root.
package loser
