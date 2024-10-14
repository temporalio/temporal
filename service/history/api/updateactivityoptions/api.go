// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package updateactivityoptions

import (
	"context"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"time"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func updateActivityOptions(
	shard shard.Context,
	mutableState workflow.MutableState,
	request *historyservice.UpdateActivityOptionsRequest,
	response *historyservice.UpdateActivityOptionsResponse,
) (*api.UpdateWorkflowAction, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	updateRequest := request.GetUpdateRequest()
	activityOptions := updateRequest.GetActivityOptions()
	activityId := updateRequest.GetActivityId()

	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		// Looks like ActivityTask already completed as a result of another call.
		// It is OK to drop the task at this point.
		return nil, consts.ErrActivityTaskNotFound
	}

	// update activity options
	util.ApplyFieldMask(ai, activityOptions, updateRequest.GetUpdateMask())

	// move forward activity version
	ai.Stamp += 1

	// invalidate timers
	ai.TimerTaskStatus = workflow.TimerTaskStatusNone

	// regenerate retry tasks
	if workflow.GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// two options - it can be in backoff, or waiting to be started
		now := shard.GetTimeSource().Now().In(time.UTC)
		if now.After(ai.ScheduledTime.AsTime()) {
			// activity is past its scheduled time and ready to be started
			// we don't really need to do generate timer tasks, it should be done in closeTransaction
		} else {
			// activity is in backoff
			_, err := mutableState.RetryActivity(ai, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	// fill the response
	response = &historyservice.UpdateActivityOptionsResponse{
		ActivityOptions: &activitypb.ActivityOptions{
			TaskQueue: &taskqueuepb.TaskQueue{
				Name: ai.TaskQueue,
			},
			ScheduleToCloseTimeout: ai.ScheduleToCloseTimeout,
			ScheduleToStartTimeout: ai.ScheduleToStartTimeout,
			StartToCloseTimeout:    ai.StartToCloseTimeout,
			HeartbeatTimeout:       ai.HeartbeatTimeout,
			RetryPolicy: &commonpb.RetryPolicy{
				BackoffCoefficient: ai.RetryBackoffCoefficient,
				InitialInterval:    ai.RetryInitialInterval,
				MaximumInterval:    ai.RetryMaximumInterval,
				MaximumAttempts:    ai.RetryMaximumAttempts,
			},
		},
	}

	return &api.UpdateWorkflowAction{
		Noop:               false,
		CreateWorkflowTask: false,
	}, nil
}

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateActivityOptionsRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UpdateActivityOptionsResponse, retError error) {
	_, err := api.GetActiveNamespace(shard, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	response := &historyservice.UpdateActivityOptionsResponse{}
	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetUpdateRequest().WorkflowId,
			request.GetUpdateRequest().RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			return updateActivityOptions(shard, mutableState, request, response)
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return response, err
}
