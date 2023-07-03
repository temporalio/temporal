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
	"go.temporal.io/server/common/primitives/timestamp"
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

		lastValidatedTaskInfo taskValidationInfo
	}
)

func newTaskValidator(
	historyClient historyservice.HistoryServiceClient,
) *taskValidatorImpl {
	return &taskValidatorImpl{
		historyClient: historyClient,
	}
}

// check if a task has expired / is valid
// if return false, then task is invalid and should be discarded
// if return true, then task is *not invalid*, and should be dispatched
//
// a task is invalid if this task is already failed; timeout; completed, etc
// a task is *not invalid* if this task can be started, or caller cannot verify the validity
func (v *taskValidatorImpl) maybeValidate(
	task *persistencespb.AllocatedTaskInfo,
	taskType enumspb.TaskQueueType,
) bool {
	if IsTaskExpired(task) {
		return false
	}
	if !v.shouldValidate(task) {
		return true
	}
	valid, err := v.isTaskValid(task, taskType)
	if err != nil {
		return true
	}
	v.lastValidatedTaskInfo = taskValidationInfo{
		taskID:         task.TaskId,
		validationTime: time.Now().UTC(),
	}
	return valid
}

func (v *taskValidatorImpl) shouldValidate(
	task *persistencespb.AllocatedTaskInfo,
) bool {
	if v.lastValidatedTaskInfo.taskID != task.TaskId {
		// this task has not been validated

		// after timeout attempting to dispatch the task, check whether the task is still valid
		if task.Data.CreateTime != nil && time.Since(*task.Data.CreateTime) > taskReaderValidationThreshold {
			return true
		}
	} else {
		// this task has been validated before
		if time.Since(v.lastValidatedTaskInfo.validationTime) > taskReaderValidationThreshold {
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

// TODO https://github.com/temporalio/temporal/issues/1021
//
//	there should be more validation logic here
//	1. if task has valid TTL -> TTL reached -> delete
//	2. if task has 0 TTL / no TTL -> logic need to additionally check if corresponding workflow still exists
func IsTaskExpired(t *persistencespb.AllocatedTaskInfo) bool {
	expiry := timestamp.TimeValue(t.GetData().GetExpiryTime())
	return expiry.Unix() > 0 && expiry.Before(time.Now())
}
