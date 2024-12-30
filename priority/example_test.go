package priority_test

import (
	"fmt"

	"github.com/davidvella/xp/priority"
)

// ExampleQueue_minHeap demonstrates using the priority queue as a min-heap.
func ExampleQueue_minHeap() {
	// Create a min-heap (smaller values have higher priority)
	pq := priority.NewQueue[string, int](func(a, b int) bool {
		return a < b
	})

	// Add some items
	pq.Set("task1", 5)
	pq.Set("task2", 3)
	pq.Set("task3", 7)

	// Peek at highest priority item
	key, value, exists := pq.Peek()
	if exists {
		fmt.Printf("Highest priority: %s = %d\n", key, value)
	}

	// Pop items in priority order
	for pq.Len() > 0 {
		key, value, _ := pq.Pop()
		fmt.Printf("Popped: %s = %d\n", key, value)
	}

	// Output:
	// Highest priority: task2 = 3
	// Popped: task2 = 3
	// Popped: task1 = 5
	// Popped: task3 = 7
}

// ExampleQueue_maxHeap demonstrates using the priority queue as a max-heap.
func ExampleQueue_maxHeap() {
	// Create a max-heap (larger values have higher priority)
	pq := priority.NewQueue[string, int](func(a, b int) bool {
		return a > b
	})

	// Add items
	pq.Set("A", 10)
	pq.Set("B", 20)
	pq.Set("C", 15)

	// Update priority of existing item
	pq.Set("A", 25)

	// Pop all items
	for pq.Len() > 0 {
		key, value, _ := pq.Pop()
		fmt.Printf("%s: %d\n", key, value)
	}

	// Output:
	// A: 25
	// B: 20
	// C: 15
}

// ExampleQueue_customType demonstrates using the queue with custom types.
func ExampleQueue_customType() {
	type Task struct {
		Priority int
		Name     string
	}

	// Create queue that orders tasks by priority
	pq := priority.NewQueue[string, Task](func(a, b Task) bool {
		return a.Priority < b.Priority
	})

	// Add tasks
	pq.Set("task1", Task{Priority: 2, Name: "Low priority"})
	pq.Set("task2", Task{Priority: 1, Name: "High priority"})

	// Process tasks in priority order
	for pq.Len() > 0 {
		_, task, _ := pq.Pop()
		fmt.Printf("Processing: %s (priority %d)\n", task.Name, task.Priority)
	}

	// Output:
	// Processing: High priority (priority 1)
	// Processing: Low priority (priority 2)
}
