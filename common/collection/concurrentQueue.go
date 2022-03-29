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
	concurrentQueueImpl struct {
		sync.Mutex
		items []interface{}
	}
)

// NewConcurrentQueue creates a new concurrent queue
func NewConcurrentQueue() Queue {
	return &concurrentQueueImpl{
		items: make([]interface{}, 0, 1000),
	}
}

func (q *concurrentQueueImpl) Peek() interface{} {
	q.Lock()
	defer q.Unlock()

	if q.isEmptyLocked() {
		return nil
	}
	return q.items[0]
}

func (q *concurrentQueueImpl) Add(item interface{}) {
	if item == nil {
		panic("cannot add nil item to queue")
	}

	q.Lock()
	defer q.Unlock()

	q.items = append(q.items, item)
}

func (q *concurrentQueueImpl) Remove() interface{} {
	q.Lock()
	defer q.Unlock()
	if q.isEmptyLocked() {
		return nil
	}

	item := q.items[0]
	q.items[0] = nil
	q.items = q.items[1:]

	return item
}

func (q *concurrentQueueImpl) IsEmpty() bool {
	q.Lock()
	defer q.Unlock()

	return q.isEmptyLocked()
}

func (q *concurrentQueueImpl) Len() int {
	q.Lock()
	defer q.Unlock()

	return len(q.items)
}

func (q *concurrentQueueImpl) isEmptyLocked() bool {
	return len(q.items) == 0
}
