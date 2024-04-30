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
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	ctasks "go.temporal.io/server/common/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executable_task_tracker_mock.go

type (
	TrackableExecutableTask interface {
		ctasks.Task
		QueueID() interface{}
		TaskID() int64
		TaskCreationTime() time.Time
		MarkPoisonPill() error
	}
	WatermarkInfo struct {
		Watermark int64
		Timestamp time.Time
	}
	ExecutableTaskTracker interface {
		TrackTasks(exclusiveHighWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask) []TrackableExecutableTask
		LowWatermark() *WatermarkInfo
		Size() int
		Cancel()
	}
	ExecutableTaskTrackerImpl struct {
		logger         log.Logger
		metricsHandler metrics.Handler

		sync.Mutex
		cancelled                  bool
		exclusiveHighWatermarkInfo *WatermarkInfo // this is exclusive, i.e. source need to resend with this watermark / task ID
		taskQueue                  *list.List     // sorted by task ID
	}
)

var _ ExecutableTaskTracker = (*ExecutableTaskTrackerImpl)(nil)

func NewExecutableTaskTracker(
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ExecutableTaskTrackerImpl {
	return &ExecutableTaskTrackerImpl{
		logger:         logger,
		metricsHandler: metricsHandler,

		exclusiveHighWatermarkInfo: nil,
		taskQueue:                  list.New(),
	}
}

// TrackTasks add tasks for tracking, return valid tasks (dedup)
// if task tracker is cancelled, then newly added tasks will also be cancelled
// tasks should be sorted by task ID, all task IDs < exclusiveHighWatermarkInfo
func (t *ExecutableTaskTrackerImpl) TrackTasks(
	exclusiveHighWatermarkInfo WatermarkInfo,
	tasks ...TrackableExecutableTask,
) []TrackableExecutableTask {
	filteredTasks := make([]TrackableExecutableTask, 0, len(tasks))

	t.Lock()
	defer t.Unlock()

	// need to assume source side send replication tasks in order
	if t.exclusiveHighWatermarkInfo != nil && exclusiveHighWatermarkInfo.Watermark <= t.exclusiveHighWatermarkInfo.Watermark {
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
		filteredTasks = append(filteredTasks, task)
		lastTaskID = task.TaskID()
	}

	if exclusiveHighWatermarkInfo.Watermark <= lastTaskID {
		panic(fmt.Sprintf(
			"ExecutableTaskTracker encountered lower high watermark: %v < %v",
			exclusiveHighWatermarkInfo.Watermark,
			lastTaskID,
		))
	}
	t.exclusiveHighWatermarkInfo = &exclusiveHighWatermarkInfo

	if t.cancelled {
		t.cancelLocked()
	}
	return filteredTasks
}

func (t *ExecutableTaskTrackerImpl) LowWatermark() *WatermarkInfo {
	t.Lock()
	defer t.Unlock()

	element := t.taskQueue.Front()
Loop:
	for element != nil {
		task := element.Value.(TrackableExecutableTask)
		taskState := task.State()
		switch taskState {
		case ctasks.TaskStateAcked:
			nextElement := element.Next()
			t.taskQueue.Remove(element)
			element = nextElement
		case ctasks.TaskStateNacked:
			if err := task.MarkPoisonPill(); err != nil {
				t.logger.Error("unable to save poison pill", tag.Error(err), tag.TaskID(task.TaskID()))
				metrics.ReplicationDLQFailed.With(t.metricsHandler).Record(
					1,
					metrics.OperationTag(metrics.ReplicationTaskTrackerScope),
				)
				// unable to save poison pill, retry later
				element = element.Next()
				continue Loop
			}
			nextElement := element.Next()
			t.taskQueue.Remove(element)
			element = nextElement
		case ctasks.TaskStateAborted:
			// noop, do not remove from queue, let it block low watermark
			element = element.Next()
		case ctasks.TaskStateCancelled:
			// noop, do not remove from queue, let it block low watermark
			element = element.Next()
		case ctasks.TaskStatePending:
			// noop, do not remove from queue, let it block low watermark
			element = element.Next()
		default:
			panic(fmt.Sprintf(
				"ExecutableTaskTracker encountered unknown task state: %v",
				taskState,
			))
		}
	}

	if element := t.taskQueue.Front(); element != nil {
		inclusiveLowWatermarkInfo := WatermarkInfo{
			Watermark: element.Value.(TrackableExecutableTask).TaskID(),
			Timestamp: element.Value.(TrackableExecutableTask).TaskCreationTime(),
		}
		return &inclusiveLowWatermarkInfo
	} else if t.exclusiveHighWatermarkInfo != nil {
		inclusiveLowWatermarkInfo := *t.exclusiveHighWatermarkInfo
		return &inclusiveLowWatermarkInfo
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
