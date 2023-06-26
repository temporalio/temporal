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

package matching

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/worker/scanner/taskqueue"
)

const (
	taskReaderOfferTimeout        = 60 * time.Second
	taskReaderValidationThreshold = 600 * time.Second
)

type (
	taskValidator interface {
		maybeValidate(
			task *persistencespb.AllocatedTaskInfo,
			taskType enumspb.TaskQueueType,
		) bool
	}

	taskValidationInfo struct {
		taskID         int64
		validationTime time.Time
	}

	taskValidatorImpl struct {
		historyClient historyservice.HistoryServiceClient

		lastValidatedTaskInfo *taskValidationInfo
	}
)

func newTaskValidator(
	historyClient historyservice.HistoryServiceClient,
) *taskValidatorImpl {
	return &taskValidatorImpl{
		historyClient: historyClient,
	}
}

// probabilistically validate whether a task has expired
// if return false, nil, then task is invalid and should be discarded
// if return true, nil, then task maybe valid, and should be dispatched
func (v *taskValidatorImpl) maybeValidate(
	task *persistencespb.AllocatedTaskInfo,
	taskType enumspb.TaskQueueType,
) bool {
	if taskqueue.IsTaskExpired(task) {
		return false
	}
	if !v.shouldValidate(task) {
		return true
	}
	valid, err := v.isTaskValid(task, taskType)
	if err != nil {
		return true
	}
	v.lastValidatedTaskInfo = &taskValidationInfo{
		taskID:         task.TaskId,
		validationTime: time.Now().UTC(),
	}
	return valid
}

func (v *taskValidatorImpl) shouldValidate(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	if v.lastValidatedTaskInfo == nil || v.lastValidatedTaskInfo.taskID != task.TaskId {
		// this task has not been validated

		// after timeout attempting to dispatch the task, checking whether the task is still valid
		if task.Data.CreateTime != nil && time.Now().Sub(*task.Data.CreateTime) > taskReaderValidationThreshold {
			return true
		}
	} else {
		// this task has been validated before
		if time.Now().Sub(v.lastValidatedTaskInfo.validationTime) > taskReaderValidationThreshold {
			return true
		}
	}
	return false
}

func (v *taskValidatorImpl) isTaskValid(
	task *persistencespb.AllocatedTaskInfo,
	taskType enumspb.TaskQueueType,
) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	namespaceID := task.Data.NamespaceId
	workflowID := task.Data.WorkflowId
	runID := task.Data.RunId

	switch taskType {
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := v.historyClient.IsActivityTaskValid(ctx, &historyservice.IsActivityTaskValidRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Clock:            task.Data.Clock,
			ScheduledEventId: task.Data.ScheduledEventId,
		})
		switch err.(type) {
		case nil:
			return resp.IsValid, nil
		case *serviceerror.NotFound:
			return false, nil
		default:
			return false, err
		}
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := v.historyClient.IsWorkflowTaskValid(ctx, &historyservice.IsWorkflowTaskValidRequest{
			NamespaceId: namespaceID,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Clock:            task.Data.Clock,
			ScheduledEventId: task.Data.ScheduledEventId,
		})
		switch err.(type) {
		case nil:
			return resp.IsValid, nil
		case *serviceerror.NotFound:
			return false, nil
		default:
			return false, err
		}
	default:
		return true, nil
	}
}
