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
