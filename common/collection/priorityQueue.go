// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
func NewPriorityQueue[T any](compareLess func(this T, other T) bool) Queue[T] {
	return &priorityQueueImpl[T]{
		compareLess: compareLess,
	}
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
