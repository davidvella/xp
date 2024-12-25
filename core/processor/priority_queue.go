package processor

// item represents an item in the value queue
type item[K comparable, V any] struct {
	key   K
	value V
	index int
}

// PriorityQueue implements a value queue using a heap
type PriorityQueue[K comparable, P any] struct {
	items   []*item[K, P]
	itemMap map[K]*item[K, P]
	lessF   func(a, b P) bool // returns true if a has higher priority than b
}

// NewPriorityQueue creates a new priority queue with the given comparator
func NewPriorityQueue[K comparable, P any](less func(a, b P) bool) *PriorityQueue[K, P] {
	return &PriorityQueue[K, P]{
		items:   make([]*item[K, P], 0),
		itemMap: make(map[K]*item[K, P]),
		lessF:   less,
	}
}

// Len returns the number of items in the queue
func (pq *PriorityQueue[K, P]) Len() int {
	return len(pq.items)
}

// Get adds a new key or updates an existing key's value
func (pq *PriorityQueue[K, P]) Get(key K) (P, bool) {
	i, exists := pq.itemMap[key]
	if !exists {
		var zeroP P
		return zeroP, exists
	}
	return i.value, exists
}

// Set adds a new key or updates an existing key's value
func (pq *PriorityQueue[K, P]) Set(key K, value P) {
	if i, exists := pq.itemMap[key]; exists {
		oldValue := i.value
		i.value = value
		if pq.lessF(value, oldValue) {
			pq.up(i.index)
		} else {
			pq.down(i.index)
		}
	} else {
		i := &item[K, P]{
			key:   key,
			value: value,
			index: len(pq.items),
		}
		pq.items = append(pq.items, i)
		pq.itemMap[key] = i
		pq.up(i.index)
	}
}

// Remove removes the given key from the queue
func (pq *PriorityQueue[K, P]) Remove(key K) {
	i, exists := pq.itemMap[key]
	if !exists {
		panic("key does not exist")
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

// Pop removes and returns the highest priority item
func (pq *PriorityQueue[K, P]) Pop() (K, P, bool) {
	if len(pq.items) == 0 {
		var zeroK K
		var zeroP P
		return zeroK, zeroP, false
	}

	i := pq.items[0]
	pq.Remove(i.key)
	return i.key, i.value, true
}

// Peek returns the highest priority item without removing it
func (pq *PriorityQueue[K, P]) Peek() (K, P, bool) {
	if len(pq.items) == 0 {
		var zeroK K
		var zeroP P
		return zeroK, zeroP, false
	}
	i := pq.items[0]
	return i.key, i.value, true
}

// swap swaps items at index i and j
func (pq *PriorityQueue[K, P]) swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
	pq.items[i].index = i
	pq.items[j].index = j
}

// less compares items at index i and j
func (pq *PriorityQueue[K, P]) less(i, j int) bool {
	return pq.lessF(pq.items[i].value, pq.items[j].value)
}

// up moves the element at index i up to its proper position
func (pq *PriorityQueue[K, P]) up(i int) {
	for {
		parent := (i - 1) / 2
		if parent == i || !pq.less(i, parent) {
			break
		}
		pq.swap(i, parent)
		i = parent
	}
}

// down moves the element at index i down to its proper position
func (pq *PriorityQueue[K, P]) down(i int) {
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
