package priority

// item represents an item in the value queue.
type item[K comparable, V any] struct {
	key   K
	value V
	index int
}

// Queue implements a value queue using a heap.
type Queue[K comparable, V any] struct {
	items   []*item[K, V]
	itemMap map[K]*item[K, V]
	lessF   func(a, b V) bool // returns true if a has higher priority than b
}

// NewQueue creates a new priority queue with the given comparator.
func NewQueue[K comparable, P any](less func(a, b P) bool) *Queue[K, P] {
	return &Queue[K, P]{
		items:   make([]*item[K, P], 0),
		itemMap: make(map[K]*item[K, P]),
		lessF:   less,
	}
}

// Len returns the number of items in the queue.
func (pq *Queue[K, V]) Len() int {
	return len(pq.items)
}

// Get adds a new key or updates an existing key's value.
func (pq *Queue[K, V]) Get(key K) (V, bool) {
	i, exists := pq.itemMap[key]
	if !exists {
		var zeroP V
		return zeroP, exists
	}
	return i.value, exists
}

// Set adds a new key or updates an existing key's value.
func (pq *Queue[K, V]) Set(key K, value V) {
	if i, exists := pq.itemMap[key]; exists {
		oldValue := i.value
		i.value = value
		if pq.lessF(value, oldValue) {
			pq.up(i.index)
		} else {
			pq.down(i.index)
		}
	} else {
		i := &item[K, V]{
			key:   key,
			value: value,
			index: len(pq.items),
		}
		pq.items = append(pq.items, i)
		pq.itemMap[key] = i
		pq.up(i.index)
	}
}

// Remove removes the given key from the queue.
func (pq *Queue[K, V]) Remove(key K) {
	i, exists := pq.itemMap[key]
	if !exists {
		return
	}

	idx := i.index
	lastIdx := len(pq.items) - 1

	if idx != lastIdx {
		pq.swap(idx, lastIdx)
		pq.items = pq.items[:lastIdx]
		if idx < lastIdx {
			pq.down(idx)
			pq.up(idx)
		}
	} else {
		pq.items = pq.items[:lastIdx]
	}

	delete(pq.itemMap, key)
}

// Pop removes and returns the highest priority item.
func (pq *Queue[K, V]) Pop() (key K, value V, exists bool) {
	if len(pq.items) == 0 {
		var zeroK K
		var zeroP V
		return zeroK, zeroP, false
	}

	i := pq.items[0]
	pq.Remove(i.key)
	return i.key, i.value, true
}

// Peek returns the highest priority item without removing it.
func (pq *Queue[K, V]) Peek() (key K, value V, exists bool) {
	if len(pq.items) == 0 {
		var zeroK K
		var zeroP V
		return zeroK, zeroP, false
	}
	i := pq.items[0]
	return i.key, i.value, true
}

// swap swaps items at index i and j.
func (pq *Queue[K, V]) swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

// less compares items at index i and j.
func (pq *Queue[K, V]) less(i, j int) bool {
	return pq.lessF(pq.items[i].value, pq.items[j].value)
}

// up moves the element at index i up to its proper position.
func (pq *Queue[K, V]) up(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !pq.less(i, parent) {
			break
		}
		pq.swap(i, parent)
		i = parent
	}
}

// down moves the element at index i down to its proper position.
func (pq *Queue[K, V]) down(i int) {
	for {
		smallest := i
		left := 2*i + 1
		right := 2*i + 2

		if left < len(pq.items) && pq.less(left, smallest) {
			smallest = left
		}
		if right < len(pq.items) && pq.less(right, smallest) {
			smallest = right
		}

		if smallest == i {
			break
		}

		pq.swap(i, smallest)
		i = smallest
	}
}
