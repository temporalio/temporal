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

package replication

import (
	"container/list"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/server/common/log"
	ctasks "go.temporal.io/server/common/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_task_tracker_mock.go

type (
	TrackableExecutableTask interface {
		ctasks.Task
		TaskID() int64
		TaskCreationTime() time.Time
		MarkPoisonPill() error
	}
	WatermarkInfo struct {
		Watermark int64
		Timestamp time.Time
	}
	ExecutableTaskTracker interface {
		TrackTasks(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask
		LowWatermark() *WatermarkInfo
		Size() int
		Cancel()
	}
	ExecutableTaskTrackerImpl struct {
		logger log.Logger

		sync.Mutex
		cancelled         bool
		highWatermarkInfo *WatermarkInfo // this is exclusive, i.e. source need to resend with this watermark / task ID
		taskQueue         *list.List     // sorted by task ID
		taskIDs           map[int64]struct{}
	}
)

var _ ExecutableTaskTracker = (*ExecutableTaskTrackerImpl)(nil)

func NewExecutableTaskTracker(
	logger log.Logger,
) *ExecutableTaskTrackerImpl {
	return &ExecutableTaskTrackerImpl{
		logger: logger,

		highWatermarkInfo: nil,
		taskQueue:         list.New(),
		taskIDs:           make(map[int64]struct{}),
	}
}

// TrackTasks add tasks for tracking, return valid tasks (dedup)
// if task tracker is cancelled, then newly added tasks will also be cancelled
func (t *ExecutableTaskTrackerImpl) TrackTasks(
	highWatermarkInfo WatermarkInfo,
	tasks ...TrackableExecutableTask,
) []TrackableExecutableTask {
	filteredTasks := make([]TrackableExecutableTask, 0, len(tasks))

	t.Lock()
	defer t.Unlock()

	// need to assume source side send replication tasks in order
	if t.highWatermarkInfo != nil && highWatermarkInfo.Watermark <= t.highWatermarkInfo.Watermark {
		return filteredTasks
	}

	lastTaskID := int64(-1)
	if item := t.taskQueue.Back(); item != nil {
		lastTaskID = item.Value.(TrackableExecutableTask).TaskID()
	}
Loop:
	for _, task := range tasks {
		if lastTaskID >= task.TaskID() {
			// need to assume source side send replication tasks in order
			continue Loop
		}
		t.taskQueue.PushBack(task)
		t.taskIDs[task.TaskID()] = struct{}{}
		filteredTasks = append(filteredTasks, task)
		lastTaskID = task.TaskID()
	}

	if highWatermarkInfo.Watermark < lastTaskID {
		panic(fmt.Sprintf(
			"ExecutableTaskTracker encountered lower high watermark: %v < %v",
			highWatermarkInfo.Watermark,
			lastTaskID,
		))
	}
	t.highWatermarkInfo = &highWatermarkInfo

	if t.cancelled {
		t.cancelLocked()
	}
	return filteredTasks
}

func (t *ExecutableTaskTrackerImpl) LowWatermark() *WatermarkInfo {
	t.Lock()
	defer t.Unlock()

Loop:
	for element := t.taskQueue.Front(); element != nil; element = element.Next() {
		task := element.Value.(TrackableExecutableTask)
		taskState := task.State()
		switch taskState {
		case ctasks.TaskStateAcked:
			delete(t.taskIDs, task.TaskID())
			t.taskQueue.Remove(element)
		case ctasks.TaskStateNacked:
			if err := task.MarkPoisonPill(); err != nil {
				// unable to save poison pill, retry later
				break Loop
			}
			delete(t.taskIDs, task.TaskID())
			t.taskQueue.Remove(element)
		case ctasks.TaskStateAborted:
			// noop, do not remove from queue, let it block low watermark
			break Loop
		case ctasks.TaskStateCancelled:
			// noop, do not remove from queue, let it block low watermark
			break Loop
		case ctasks.TaskStatePending:
			// noop, do not remove from queue, let it block low watermark
			break Loop
		default:
			panic(fmt.Sprintf(
				"ExecutableTaskTracker encountered unknown task state: %v",
				taskState,
			))
		}
	}

	if element := t.taskQueue.Front(); element != nil {
		lowWatermarkInfo := WatermarkInfo{
			Watermark: element.Value.(TrackableExecutableTask).TaskID(),
			Timestamp: element.Value.(TrackableExecutableTask).TaskCreationTime(),
		}
		return &lowWatermarkInfo
	} else if t.highWatermarkInfo != nil {
		lowWatermarkInfo := *t.highWatermarkInfo
		return &lowWatermarkInfo
	} else {
		return nil
	}
}

func (t *ExecutableTaskTrackerImpl) Size() int {
	t.Lock()
	defer t.Unlock()

	return t.taskQueue.Len()
}

func (t *ExecutableTaskTrackerImpl) Cancel() {
	t.Lock()
	defer t.Unlock()

	t.cancelled = true
	t.cancelLocked()
}

func (t *ExecutableTaskTrackerImpl) cancelLocked() {
	for element := t.taskQueue.Front(); element != nil; element = element.Next() {
		task := element.Value.(TrackableExecutableTask)
		task.Cancel()
	}
}
