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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/tasks"
)

const (
	targetLoadFactor           = 0.8
	clearSliceThrottleDuration = 10 * time.Second
)

type (
	actionQueuePendingTask struct {
		attributes     *AlertAttributesQueuePendingTaskCount
		monitor        Monitor
		maxReaderCount int
		completionFn   actionCompletionFn

		// state of the action, used when running the action
		tasksPerNamespace               map[namespace.ID]int
		pendingTaskPerNamespacePerSlice map[Slice]map[namespace.ID]int
		slicesPerNamespace              map[namespace.ID][]Slice
		namespaceToClearPerSlice        map[Slice][]namespace.ID
	}
)

func newQueuePendingTaskAction(
	attributes *AlertAttributesQueuePendingTaskCount,
	monitor Monitor,
	maxReaderCount int,
	completionFn actionCompletionFn,
) Action {
	return &actionQueuePendingTask{
		attributes:     attributes,
		monitor:        monitor,
		maxReaderCount: maxReaderCount,
		completionFn:   completionFn,
	}
}

func (a *actionQueuePendingTask) Run(readerGroup *ReaderGroup) {
	defer a.completionFn()

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
	readers map[int32]Reader,
) bool {
	for _, reader := range readers {
		reader.ShrinkSlices()
	}
	return a.monitor.GetTotalPendingTaskCount() <= a.attributes.CiriticalPendingTaskCount
}

func (a *actionQueuePendingTask) init() {
	a.tasksPerNamespace = make(map[namespace.ID]int)
	a.pendingTaskPerNamespacePerSlice = make(map[Slice]map[namespace.ID]int)
	a.slicesPerNamespace = make(map[namespace.ID][]Slice)
	a.namespaceToClearPerSlice = make(map[Slice][]namespace.ID)
}

func (a *actionQueuePendingTask) gatherStatistics(
	readers map[int32]Reader,
) {
	// gather statistic for
	// 1. total # of pending tasks per namespace
	// 2. for each slice, # of pending taks per namespace
	// 3. for each namespace, a list of slices that contains pending tasks from that namespace,
	//    reversely ordered by slice range. Upon unloading, first unload newer slices.
	for _, reader := range readers {
		reader.WalkSlices(func(s Slice) {
			a.pendingTaskPerNamespacePerSlice[s] = s.TaskStats().PendingPerNamespace
			for namespaceID, pendingTaskCount := range a.pendingTaskPerNamespacePerSlice[s] {
				a.tasksPerNamespace[namespaceID] += pendingTaskCount
				a.slicesPerNamespace[namespaceID] = append(a.slicesPerNamespace[namespaceID], s)
			}
		})
	}
	for _, sliceList := range a.slicesPerNamespace {
		slices.SortFunc(sliceList, func(this, that Slice) bool {
			thisMin := this.Scope().Range.InclusiveMin
			thatMin := that.Scope().Range.InclusiveMin
			return thisMin.CompareTo(thatMin) > 0
		})
	}
}

func (a *actionQueuePendingTask) findSliceToClear(
	targetPendingTasks int,
) {
	currentPendingTasks := 0
	// order namespace by # of pending tasks
	namespaceIDs := make([]namespace.ID, 0, len(a.tasksPerNamespace))
	for namespaceID, namespacePendingTasks := range a.tasksPerNamespace {
		currentPendingTasks += namespacePendingTasks
		namespaceIDs = append(namespaceIDs, namespaceID)
	}
	pq := collection.NewPriorityQueueWithItems(
		func(this, that namespace.ID) bool {
			return a.tasksPerNamespace[this] > a.tasksPerNamespace[that]
		},
		namespaceIDs,
	)

	for currentPendingTasks > targetPendingTasks && !pq.IsEmpty() {
		namespaceID := pq.Remove()

		sliceList := a.slicesPerNamespace[namespaceID]
		if len(sliceList) == 0 {
			panic("Found namespace with non-zero pending task count but has no correspoding Slice")
		}

		// pop the first slice in the list
		sliceToClear := sliceList[0]
		sliceList = sliceList[1:]
		a.slicesPerNamespace[namespaceID] = sliceList

		tasksCleared := a.pendingTaskPerNamespacePerSlice[sliceToClear][namespaceID]
		a.tasksPerNamespace[namespaceID] -= tasksCleared
		currentPendingTasks -= tasksCleared
		if a.tasksPerNamespace[namespaceID] > 0 {
			pq.Add(namespaceID)
		}

		a.namespaceToClearPerSlice[sliceToClear] = append(a.namespaceToClearPerSlice[sliceToClear], namespaceID)
	}
}

func (a *actionQueuePendingTask) splitAndClearSlice(
	readers map[int32]Reader,
	readerGroup *ReaderGroup,
) {
	for readerID, reader := range readers {
		if readerID == int32(a.maxReaderCount)-1 {
			// we can't do further split, have to clear entire slice
			cleared := false
			reader.ClearSlices(func(s Slice) bool {
				_, ok := a.namespaceToClearPerSlice[s]
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
			namespaceIDs, ok := a.namespaceToClearPerSlice[s]
			if !ok {
				return nil, false
			}

			namespaceIDStrings := make([]string, 0, len(namespaceIDs))
			for _, namespaceID := range namespaceIDs {
				namespaceIDStrings = append(namespaceIDStrings, namespaceID.String())
			}

			split, remain := s.SplitByPredicate(tasks.NewNamespacePredicate(namespaceIDStrings))
			split.Clear()
			splitSlices = append(splitSlices, split)
			return []Slice{remain}, true
		})

		if len(splitSlices) == 0 {
			continue
		}

		nextReader, ok := readerGroup.ReaderByID(readerID + 1)
		if ok {
			nextReader.MergeSlices(splitSlices...)
		} else {
			nextReader = readerGroup.NewReader(readerID+1, splitSlices...)
		}
		nextReader.Pause(clearSliceThrottleDuration)
	}

	// it's likely that after a split, slice range can be shrinked
	// as tasks blocking the min key from moving have been moved to another slice/reader
	for _, reader := range readers {
		reader.ShrinkSlices()
	}
}
