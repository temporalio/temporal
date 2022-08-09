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

package tasks

import (
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

func Tags(
	task Task,
) []tag.Tag {
	// TODO: convert this to a method GetEventID on task interface
	// or remove this tag as the value is visible in the Task tag value.
	taskEventID := int64(0)
	taskCategory := task.GetCategory()
	switch taskCategory.ID() {
	case CategoryIDTransfer:
		taskEventID = GetTransferTaskEventID(task)
	case CategoryIDTimer:
		taskEventID = GetTimerTaskEventID(task)
	default:
		// no-op, other task categories don't have task eventID
	}

	return []tag.Tag{
		tag.WorkflowNamespaceID(task.GetNamespaceID()),
		tag.WorkflowID(task.GetWorkflowID()),
		tag.WorkflowRunID(task.GetRunID()),
		tag.TaskID(task.GetTaskID()),
		tag.TaskVisibilityTimestamp(task.GetVisibilityTime()),
		tag.TaskType(task.GetType()),
		tag.Task(task),
		tag.WorkflowEventID(taskEventID),
	}
}

// TODO: deprecate this method, use logger from executable.Logger() instead
func InitializeLogger(
	task Task,
	logger log.Logger,
) log.Logger {
	return log.With(
		logger,
		Tags(task)...,
	)
}

func GetTransferTaskEventID(
	transferTask Task,
) int64 {
	eventID := int64(0)
	switch task := transferTask.(type) {
	case *ActivityTask:
		eventID = task.ScheduledEventID
	case *WorkflowTask:
		eventID = task.ScheduledEventID
	case *CloseExecutionTask:
		eventID = common.FirstEventID
	case *DeleteExecutionTask:
		eventID = common.FirstEventID
	case *CancelExecutionTask:
		eventID = task.InitiatedEventID
	case *SignalExecutionTask:
		eventID = task.InitiatedEventID
	case *StartChildExecutionTask:
		eventID = task.InitiatedEventID
	case *ResetWorkflowTask:
		eventID = common.FirstEventID
	case *fakeTask:
		// no-op
	default:
		panic(serviceerror.NewInternal("unknown transfer task"))
	}
	return eventID
}

func GetTimerTaskEventID(
	timerTask Task,
) int64 {
	eventID := int64(0)

	switch task := timerTask.(type) {
	case *UserTimerTask:
		eventID = task.EventID
	case *ActivityTimeoutTask:
		eventID = task.EventID
	case *WorkflowTaskTimeoutTask:
		eventID = task.EventID
	case *WorkflowBackoffTimerTask:
		eventID = common.FirstEventID
	case *ActivityRetryTimerTask:
		eventID = task.EventID
	case *WorkflowTimeoutTask:
		eventID = common.FirstEventID
	case *DeleteHistoryEventTask:
		eventID = common.FirstEventID
	case *fakeTask:
		// no-op
	default:
		panic(serviceerror.NewInternal("unknown timer task"))
	}
	return eventID
}
