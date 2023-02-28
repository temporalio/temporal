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

type (
	TrackableExecutableTask interface {
		ctasks.Task
		TaskID() int64
		TaskCreationTime() time.Time
	}
	WatermarkInfo struct {
		Watermark int64
		Timestamp time.Time
	}
	ExecutableTaskTracker interface {
		TrackTasks(highWatermarkInfo WatermarkInfo, tasks ...TrackableExecutableTask)
		LowWatermark() *WatermarkInfo
	}
	ExecutableTaskTrackerImpl struct {
		logger log.Logger

		sync.Mutex
		highWatermarkInfo *WatermarkInfo
		taskQueue         *list.List // sorted by task ID
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
	}
}

func (t *ExecutableTaskTrackerImpl) TrackTasks(
	highWatermarkInfo WatermarkInfo,
	tasks ...TrackableExecutableTask,
) {
	t.Lock()
	defer t.Unlock()

	lastTaskID := int64(0)
	if item := t.taskQueue.Back(); item != nil {
		lastTaskID = item.Value.(TrackableExecutableTask).TaskID()
	}
	for _, task := range tasks {
		if lastTaskID >= task.TaskID() {
			panic(fmt.Sprintf(
				"ExecutableTaskTracker encountered out of order task, ID: %v",
				task.TaskID(),
			))
		}
		t.taskQueue.PushBack(task)
	}

	if t.highWatermarkInfo != nil && highWatermarkInfo.Watermark < t.highWatermarkInfo.Watermark {
		panic(fmt.Sprintf(
			"ExecutableTaskTracker encountered lower high watermark: %v < %v",
			highWatermarkInfo.Watermark,
			t.highWatermarkInfo.Watermark,
		))
	}
	t.highWatermarkInfo = &highWatermarkInfo
}

func (t *ExecutableTaskTrackerImpl) LowWatermark() *WatermarkInfo {
	t.Lock()
	defer t.Unlock()

	for element := t.taskQueue.Front(); element != nil; element = element.Next() {
		task := element.Value.(TrackableExecutableTask)
		taskState := task.State()
		switch taskState {
		case ctasks.TaskStateAcked:
			t.taskQueue.Remove(element)
		case ctasks.TaskStateNacked:
			// TODO put to DLQ, only after <- is successful, then remove from tracker
			panic("implement me")
		case ctasks.TaskStateCancelled:
			// noop, do not remove from queue, let it block low watermark
		case ctasks.TaskStatePending:
			// noop, do not remove from queue, let it block low watermark
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
