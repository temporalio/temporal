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
	"sync"
)

type (
	concurrentPriorityQueueImpl struct {
		lock          sync.Mutex
		priorityQueue Queue
	}
)

// NewConcurrentPriorityQueue create a new concurrent priority queue
func NewConcurrentPriorityQueue(compareLess func(this interface{}, other interface{}) bool) Queue {
	return &concurrentPriorityQueueImpl{
		priorityQueue: NewPriorityQueue(compareLess),
	}
}

// Peek returns the top item of the priority queue
func (pq *concurrentPriorityQueueImpl) Peek() interface{} {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.priorityQueue.Peek()
}

// Add push an item to priority queue
func (pq *concurrentPriorityQueueImpl) Add(item interface{}) {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	pq.priorityQueue.Add(item)
}

// Remove pop an item from priority queue
func (pq *concurrentPriorityQueueImpl) Remove() interface{} {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.priorityQueue.Remove()
}

// IsEmpty indicate if the priority queue is empty
func (pq *concurrentPriorityQueueImpl) IsEmpty() bool {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.priorityQueue.IsEmpty()
}

// Len return the size of the queue
func (pq *concurrentPriorityQueueImpl) Len() int {
	pq.lock.Lock()
	defer pq.lock.Unlock()

	return pq.priorityQueue.Len()
}
