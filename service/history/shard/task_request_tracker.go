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

package shard

import (
	"sync"

	"go.temporal.io/server/service/history/tasks"
)

type (
	taskRequestCompletionFn func(error)

	taskRequestTracker struct {
		sync.Mutex

		// using priority queue to track the min pending task key
		// might be an overkill since the max length of the nested map
		// is equal to shardIO concurrency limit which should be small
		pendingTaskKeys      map[tasks.Category]map[tasks.Key]struct{}
		inflightRequestCount int

		waitChannels []chan<- struct{}
	}
)

func newTaskRequestTracker(registry tasks.TaskCategoryRegistry) *taskRequestTracker {
	outstandingTaskKeys := make(map[tasks.Category]map[tasks.Key]struct{})
	for _, category := range registry.GetCategories() {
		outstandingTaskKeys[category] = make(map[tasks.Key]struct{})
	}
	return &taskRequestTracker{
		pendingTaskKeys: outstandingTaskKeys,
	}
}

func (t *taskRequestTracker) track(
	taskMaps ...map[tasks.Category][]tasks.Task,
) taskRequestCompletionFn {
	minKeyByCategory := make(map[tasks.Category]tasks.Key)
	for _, taskMap := range taskMaps {
		for category, tasksPerCategory := range taskMap {
			minKey := tasks.MaximumKey
			for _, task := range tasksPerCategory {
				if task.GetKey().CompareTo(minKey) < 0 {
					minKey = task.GetKey()
				}
			}
			if minKey.CompareTo(tasks.MaximumKey) == 0 {
				continue
			}

			if _, ok := minKeyByCategory[category]; !ok {
				minKeyByCategory[category] = minKey
			} else {
				minKeyByCategory[category] = tasks.MinKey(minKeyByCategory[category], minKey)
			}
		}
	}

	t.Lock()
	defer t.Unlock()

	t.inflightRequestCount++

	for category, minKey := range minKeyByCategory {
		t.pendingTaskKeys[category][minKey] = struct{}{}
	}

	return func(writeErr error) {
		t.Lock()
		defer t.Unlock()

		// Task key is not pending only when we get a definitive result from persistence.
		// This result can be either a success or a error that guarantees the task with that key
		// will not be persisted.
		if writeErr == nil || !OperationPossiblySucceeded(writeErr) {
			// we can only remove the task from the pending task list if we are sure it was inserted
			// or the insertion is guaranteed to have failed
			for category, minKey := range minKeyByCategory {
				delete(t.pendingTaskKeys[category], minKey)
			}
		}

		// While task key might still be pending, the request is completed and no longer inflight
		t.inflightRequestCount--
		if t.inflightRequestCount == 0 {
			t.closeWaitChannelsLocked()
		}
	}
}

func (t *taskRequestTracker) minTaskKey(
	category tasks.Category,
) (tasks.Key, bool) {
	t.Lock()
	defer t.Unlock()

	pendingTasksForCategory := t.pendingTaskKeys[category]
	if len(pendingTasksForCategory) == 0 {
		return tasks.MinimumKey, false
	}

	minKey := tasks.MaximumKey
	for taskKey := range pendingTasksForCategory {
		if taskKey.CompareTo(minKey) < 0 {
			minKey = taskKey
		}
	}

	return minKey, true
}

// drain method blocks until all inflight requests are completed
// This method should be called before updating shard rangeID,
// otherwise inflight request can fails as those requests are conditioned on
// the current rangeID
func (t *taskRequestTracker) drain() {
	t.Lock()

	if t.inflightRequestCount == 0 {
		t.Unlock()
		return
	}

	waitCh := make(chan struct{})
	t.waitChannels = append(t.waitChannels, waitCh)
	t.Unlock()

	<-waitCh
}

func (t *taskRequestTracker) clear() {
	t.Lock()
	defer t.Unlock()

	for category := range t.pendingTaskKeys {
		t.pendingTaskKeys[category] = make(map[tasks.Key]struct{})
	}
	t.inflightRequestCount = 0
	t.closeWaitChannelsLocked()
}

func (t *taskRequestTracker) closeWaitChannelsLocked() {
	for _, waitCh := range t.waitChannels {
		close(waitCh)
	}
	t.waitChannels = nil
}
