// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package tasks

import (
	"sync"
)

// SequentialTaskQueueImpl is a thread-safe implementation of SequentialTaskQueue
// that uses a slice as the underlying storage.
type SequentialTaskQueueImpl[T Task, K comparable] struct {
	sync.Mutex
	id    K
	tasks []T
}

// NewSequentialTaskQueueImpl creates a new SequentialTaskQueueImpl with the given ID.
func NewSequentialTaskQueueImpl[T Task, K comparable](id K) *SequentialTaskQueueImpl[T, K] {
	return &SequentialTaskQueueImpl[T, K]{
		id:    id,
		tasks: make([]T, 0, 8), // small initial capacity
	}
}

func (q *SequentialTaskQueueImpl[T, K]) ID() interface{} {
	return q.id
}

func (q *SequentialTaskQueueImpl[T, K]) Add(task T) {
	q.Lock()
	defer q.Unlock()
	q.tasks = append(q.tasks, task)
}

func (q *SequentialTaskQueueImpl[T, K]) Remove() T {
	q.Lock()
	defer q.Unlock()
	if len(q.tasks) == 0 {
		var zero T
		return zero
	}
	task := q.tasks[0]
	q.tasks = q.tasks[1:]
	return task
}

func (q *SequentialTaskQueueImpl[T, K]) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()
	return len(q.tasks) == 0
}

func (q *SequentialTaskQueueImpl[T, K]) Len() int {
	q.Lock()
	defer q.Unlock()
	return len(q.tasks)
}
