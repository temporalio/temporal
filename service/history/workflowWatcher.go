// Copyright (c) 2017 Uber Technologies, Inc.
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

package history

import (
	"sync"
	"sync/atomic"
)

type (
	// WatcherSnapshot contains the union of all workflow state subscribers care about.
	WatcherSnapshot struct {
		CloseStatus int
	}

	// WorkflowWatcher is used to get WatcherSnapshots when workflow updates occur.
	WorkflowWatcher interface {
		Publish(*WatcherSnapshot)
		Subscribe() (int64, <-chan struct{})
		Unsubscribe(int64)
		GetLatestSnapshot() *WatcherSnapshot
	}

	workflowWatcher struct {
		// An atomic is used to keep track of latest snapshot rather than pushing snapshots on a channel.
		// If a channel (buffered or unbuffered) is used, then at some point either blocking will occur or a snapshot will be dropped.
		// Since clients only care about getting the latest, simplify notifying clients
		// of updates and providing the ability to get the latest snapshot satisfies all current use cases.
		latestSnapshot atomic.Value
		lockableSubscribers
	}

	lockableSubscribers struct {
		sync.Mutex
		nextSubscriberID int64
		subscribers      map[int64]chan struct{}
	}
)

func (ls *lockableSubscribers) add() (int64, chan struct{}) {
	ls.Lock()
	defer ls.Unlock()
	id := ls.nextSubscriberID
	ch := make(chan struct{}, 1)
	ls.nextSubscriberID = ls.nextSubscriberID + 1
	ls.subscribers[id] = ch
	return id, ch
}

func (ls *lockableSubscribers) remove(id int64) {
	ls.Lock()
	defer ls.Unlock()
	delete(ls.subscribers, id)
}

func (ls *lockableSubscribers) notifyAll() {
	ls.Lock()
	defer ls.Unlock()
	for _, ch := range ls.subscribers {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// NewWorkflowWatcher returns new WorkflowWatcher.
// This should only be called from workflowExecutionContext.go.
func NewWorkflowWatcher() WorkflowWatcher {
	return &workflowWatcher{
		lockableSubscribers: lockableSubscribers{
			subscribers: make(map[int64]chan struct{}),
		},
	}
}

// Publish is used to indicate that the workflow has been updated and subscribers should be notified that there is a new snapshot.
func (w *workflowWatcher) Publish(update *WatcherSnapshot) {
	w.latestSnapshot.Store(update)
	w.notifyAll()
}

// Subscribe to updates.
// Every time returned channel is received on it is guaranteed that at least one new snapshot is available.
func (w *workflowWatcher) Subscribe() (int64, <-chan struct{}) {
	return w.add()
}

// Unsubscribe from updates. This must be called when client is finished watching in order to avoid resource leak.
func (w *workflowWatcher) Unsubscribe(id int64) {
	w.remove(id)
}

// GetLatestSnapshot returns the latest WatcherSnapshot.
func (w *workflowWatcher) GetLatestSnapshot() *WatcherSnapshot {
	v := w.latestSnapshot.Load()
	if v == nil {
		return nil
	}
	return v.(*WatcherSnapshot)
}
