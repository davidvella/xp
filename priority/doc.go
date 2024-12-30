// Package priority implements a generic priority queue data structure that maintains
// a collection of key-value pairs ordered by priority. The queue supports efficient
// insertion, deletion, and priority-based retrieval operations.
//
// The priority queue is implemented as a binary heap with a map for O(1) key lookups.
// The ordering is determined by a user-provided comparison function that defines the
// priority relationship between values.
//
// Key features:
//   - Generic implementation supporting any comparable key type and any value type
//   - O(log n) insertion and deletion
//   - O(1) peek operations
//   - O(1) key-based lookups
//   - Support for priority updates
//
// Basic usage:
//
//	// Create a min-heap priority queue
//	pq := priority.NewQueue[string, int](func(a, b int) bool {
//	    return a < b
//	})
//
//	// Add items
//	pq.Set("task1", 5)
//	pq.Set("task2", 3)
//	pq.Set("task3", 7)
//
//	// Get highest priority item
//	key, value, exists := pq.Peek()
//	if exists {
//	    fmt.Printf("Highest priority: %s = %d\n", key, value)
//	}
//
//	// Remove and return highest priority item
//	key, value, exists = pq.Pop()
//	if exists {
//	    fmt.Printf("Popped: %s = %d\n", key, value)
//	}
//
//	// Update priority
//	pq.Set("task1", 1)  // Updates existing key with new priority
//
//	// Remove specific key
//	pq.Remove("task2")
//
// The priority queue maintains items in a heap structure where each parent node
// has higher priority than its children (as determined by the less function).
// The less function should return true if a has higher priority than b.
package priority
