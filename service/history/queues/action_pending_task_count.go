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

package queues

import (
	"time"

	"golang.org/x/exp/slices"

	"go.temporal.io/server/common/collection"
)

const (
	targetLoadFactor           = 0.8
	clearSliceThrottleDuration = 10 * time.Second
)

var _ Action = (*actionQueuePendingTask)(nil)

type (
	actionQueuePendingTask struct {
		attributes     *AlertAttributesQueuePendingTaskCount
		monitor        Monitor
		maxReaderCount int64
		grouper        Grouper

		// Fields below this line make up the state of the action. Used when running the action.
		// Key type is "any" to support grouping tasks by arbitrary keys.
		tasksPerKey                map[any]int
		pendingTasksPerKeyPerSlice map[Slice]map[any]int
		slicesPerKey               map[any][]Slice
		keysToClearPerSlice        map[Slice][]any
	}
)

func newQueuePendingTaskAction(
	attributes *AlertAttributesQueuePendingTaskCount,
	monitor Monitor,
	maxReaderCount int,
	grouper Grouper,
) *actionQueuePendingTask {
	return &actionQueuePendingTask{
		attributes:     attributes,
		monitor:        monitor,
		maxReaderCount: int64(maxReaderCount),
		grouper:        grouper,
	}
}

func (a *actionQueuePendingTask) Name() string {
	return "queue-pending-task"
}

func (a *actionQueuePendingTask) Run(readerGroup *ReaderGroup) {
	// first check if the alert is still valid
	if a.monitor.GetTotalPendingTaskCount() <= a.attributes.CiriticalPendingTaskCount {
		return
	}

	// then try to shrink existing slices, which may reduce pending task count
	readers := readerGroup.Readers()
	if a.tryShrinkSlice(readers) {
		return
	}

	// have to unload pending tasks to reduce pending task count
	a.init()
	a.gatherStatistics(readers)
	a.findSliceToClear(
		int(float64(a.attributes.CiriticalPendingTaskCount) * targetLoadFactor),
	)
	a.splitAndClearSlice(readers, readerGroup)
}

func (a *actionQueuePendingTask) tryShrinkSlice(
	readers map[int64]Reader,
) bool {
	for _, reader := range readers {
		reader.ShrinkSlices()
	}
	return a.monitor.GetTotalPendingTaskCount() <= a.attributes.CiriticalPendingTaskCount
}

func (a *actionQueuePendingTask) init() {
	a.tasksPerKey = make(map[any]int)
	a.pendingTasksPerKeyPerSlice = make(map[Slice]map[any]int)
	a.slicesPerKey = make(map[any][]Slice)
	a.keysToClearPerSlice = make(map[Slice][]any)
}

func (a *actionQueuePendingTask) gatherStatistics(
	readers map[int64]Reader,
) {
	// gather statistic for
	// 1. total # of pending tasks per key
	// 2. for each slice, # of pending taks per key
	// 3. for each key, a list of slices that contains pending tasks from that key,
	//    reversely ordered by slice range. Upon unloading, first unload newer slices.
	for _, reader := range readers {
		reader.WalkSlices(func(s Slice) {
			pendingPerKey := s.TaskStats().PendingPerKey
			a.pendingTasksPerKeyPerSlice[s] = pendingPerKey
			for key, pendingTaskCount := range pendingPerKey {
				a.tasksPerKey[key] += pendingTaskCount
				a.slicesPerKey[key] = append(a.slicesPerKey[key], s)
			}
		})
	}
	for _, sliceList := range a.slicesPerKey {
		slices.SortFunc(sliceList, func(this, that Slice) int {
			thisMin := this.Scope().Range.InclusiveMin
			thatMin := that.Scope().Range.InclusiveMin
			// sort in largest to smallest order
			return thatMin.CompareTo(thisMin)
		})
	}
}

func (a *actionQueuePendingTask) findSliceToClear(
	targetPendingTasks int,
) {
	currentPendingTasks := 0
	// order key by # of pending tasks
	keys := make([]any, 0, len(a.tasksPerKey))
	for key, keyPendingTasks := range a.tasksPerKey {
		currentPendingTasks += keyPendingTasks
		keys = append(keys, key)
	}
	pq := collection.NewPriorityQueueWithItems(
		func(this, that any) bool {
			return a.tasksPerKey[this] > a.tasksPerKey[that]
		},
		keys,
	)

	for currentPendingTasks > targetPendingTasks && !pq.IsEmpty() {
		key := pq.Remove()

		sliceList := a.slicesPerKey[key]
		if len(sliceList) == 0 {
			panic("Found key with non-zero pending task count but has no correspoding Slice")
		}

		// pop the first slice in the list
		sliceToClear := sliceList[0]
		sliceList = sliceList[1:]
		a.slicesPerKey[key] = sliceList

		tasksCleared := a.pendingTasksPerKeyPerSlice[sliceToClear][key]
		a.tasksPerKey[key] -= tasksCleared
		currentPendingTasks -= tasksCleared
		if a.tasksPerKey[key] > 0 {
			pq.Add(key)
		}

		a.keysToClearPerSlice[sliceToClear] = append(a.keysToClearPerSlice[sliceToClear], key)
	}
}

func (a *actionQueuePendingTask) splitAndClearSlice(
	readers map[int64]Reader,
	readerGroup *ReaderGroup,
) {
	for readerID, reader := range readers {
		if readerID == int64(a.maxReaderCount)-1 {
			// we can't do further split, have to clear entire slice
			cleared := false
			reader.ClearSlices(func(s Slice) bool {
				_, ok := a.keysToClearPerSlice[s]
				cleared = cleared || ok
				return ok
			})
			if cleared {
				reader.Pause(clearSliceThrottleDuration)
			}
			continue
		}

		var splitSlices []Slice
		reader.SplitSlices(func(s Slice) ([]Slice, bool) {
			keys, ok := a.keysToClearPerSlice[s]
			if !ok {
				return nil, false
			}

			split, remain := s.SplitByPredicate(a.grouper.Predicate(keys))
			split.Clear()
			splitSlices = append(splitSlices, split)
			return []Slice{remain}, true
		})

		if len(splitSlices) == 0 {
			continue
		}

		nextReader := readerGroup.GetOrCreateReader(readerID + 1)
		nextReader.MergeSlices(splitSlices...)
		nextReader.Pause(clearSliceThrottleDuration)
	}

	// ShrinkSlices will be triggered as part of checkpointing process
	// see queueBase.handleAlert() and queueBase.checkpoint()
}
