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

package executor

import (
	"container/list"
	"sync"
)

type (
	runQueue struct {
		tasks         chan Task
		deferredTasks *threadSafeList
		stopC         chan struct{}
	}

	// threadSafeList is a wrapper around list.List which
	// guarantees thread safety
	threadSafeList struct {
		sync.Mutex
		list *list.List
	}
)

// newRunQueue returns an instance of task run queue
func newRunQueue(size int, stopC chan struct{}) *runQueue {
	return &runQueue{
		tasks:         make(chan Task, size),
		deferredTasks: newThreadSafeList(),
		stopC:         stopC,
	}
}

func (rq *runQueue) add(task Task) bool {
	select {
	case rq.tasks <- task:
		return true
	case <-rq.stopC:
		return false
	}
}

func (rq *runQueue) addAndDefer(task Task) {
	rq.deferredTasks.add(task)
}

// remove returns the next task from run queue
// if there are no active tasks in runq, a task
// from deferredQ is returned
func (rq *runQueue) remove() (Task, bool) {
	// Give 1st priority to taskQ -if there are no more tasks, handout tasks from deferredQ
	select {
	case job, ok := <-rq.tasks:
		if !ok {
			return nil, false
		}
		return job, true
	default:
		if task := rq.deferredTasks.remove(); task != nil {
			return task.Value.(Task), true
		}
	}
	// at this point, there are no tasks either in taskQ or deferredQ
	// simply block on taskQ until we have tasks
	select {
	case job, ok := <-rq.tasks:
		if !ok {
			return nil, false
		}
		return job, true
	case <-rq.stopC:
		return nil, false
	}
}

func (rq *runQueue) deferredCount() int {
	return rq.deferredTasks.len()
}

// newThreadSafeList returns a new thread safe linked list
func newThreadSafeList() *threadSafeList {
	return &threadSafeList{
		list: list.New(),
	}
}

func (tl *threadSafeList) add(elem interface{}) {
	tl.Lock()
	defer tl.Unlock()
	tl.list.PushBack(elem)
}

func (tl *threadSafeList) len() int {
	tl.Lock()
	defer tl.Unlock()
	return tl.list.Len()
}

func (tl *threadSafeList) remove() *list.Element {
	tl.Lock()
	defer tl.Unlock()
	e := tl.list.Front()
	if e != nil {
		tl.list.Remove(e)
	}
	return e
}
