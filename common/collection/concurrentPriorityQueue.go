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
