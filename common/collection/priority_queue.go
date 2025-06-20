package collection

import (
	"container/heap"
)

type (
	priorityQueueImpl[T any] struct {
		compareLess func(this T, other T) bool
		items       []T
	}
)

// NewPriorityQueue create a new priority queue
func NewPriorityQueue[T any](
	compareLess func(this T, other T) bool,
) Queue[T] {
	return &priorityQueueImpl[T]{
		compareLess: compareLess,
	}
}

// NewPriorityQueueWithItems creats a new priority queue
// with the provided list of items.
// PriorityQueue will take ownership of the passed in items,
// so caller should stop modifying it.
// The complexity is O(n) where n is the number of items
func NewPriorityQueueWithItems[T any](
	compareLess func(this T, other T) bool,
	items []T,
) Queue[T] {
	pq := &priorityQueueImpl[T]{
		compareLess: compareLess,
		items:       items,
	}
	heap.Init(pq)
	return pq
}

// Peek returns the top item of the priority queue
func (pq *priorityQueueImpl[T]) Peek() T {
	if pq.IsEmpty() {
		panic("Cannot peek item because priority queue is empty")
	}
	return pq.items[0]
}

// Add push an item to priority queue
func (pq *priorityQueueImpl[T]) Add(item T) {
	heap.Push(pq, item)
}

// Remove pop an item from priority queue
func (pq *priorityQueueImpl[T]) Remove() T {
	return heap.Pop(pq).(T)
}

// IsEmpty indicate if the priority queue is empty
func (pq *priorityQueueImpl[T]) IsEmpty() bool {
	return pq.Len() == 0
}

// below are the functions used by heap.Interface and go internal heap implementation

// Len implements sort.Interface
func (pq *priorityQueueImpl[T]) Len() int {
	return len(pq.items)
}

// Less implements sort.Interface
func (pq *priorityQueueImpl[T]) Less(i, j int) bool {
	return pq.compareLess(pq.items[i], pq.items[j])
}

// Swap implements sort.Interface
func (pq *priorityQueueImpl[T]) Swap(i, j int) {
	pq.items[i], pq.items[j] = pq.items[j], pq.items[i]
}

// Push push an item to priority queue, used by go internal heap implementation
func (pq *priorityQueueImpl[T]) Push(item interface{}) {
	pq.items = append(pq.items, item.(T))
}

// Pop pop an item from priority queue, used by go internal heap implementation
func (pq *priorityQueueImpl[T]) Pop() interface{} {
	pqItem := pq.items[pq.Len()-1]
	pq.items = pq.items[0 : pq.Len()-1]
	return pqItem
}
