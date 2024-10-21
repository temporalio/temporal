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
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UpdateActivityOptionsRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UpdateActivityOptionsResponse, retError error) {
	validator := api.NewCommandAttrValidator(
		shardContext.GetNamespaceRegistry(),
		shardContext.GetConfig(),
		nil,
	)

	response := &historyservice.UpdateActivityOptionsResponse{}
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetUpdateRequest().WorkflowId,
			request.GetUpdateRequest().RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			return updateActivityOptions(shardContext, validator, mutableState, request, response)
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	return response, err
}

func updateActivityOptions(
	shardContext shard.Context,
	validator *api.CommandAttrValidator,
	mutableState workflow.MutableState,
	request *historyservice.UpdateActivityOptionsRequest,
	response *historyservice.UpdateActivityOptionsResponse,
) (*api.UpdateWorkflowAction, error) {
	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	updateRequest := request.GetUpdateRequest()
	mergeFrom := updateRequest.GetActivityOptions()
	if mergeFrom == nil {
		return nil, serviceerror.NewInvalidArgument("ActivityOptions are not provided")
	}
	activityId := updateRequest.GetActivityId()

	ai, activityFound := mutableState.GetActivityByActivityID(activityId)

	if !activityFound {
		return nil, consts.ErrActivityNotFound
	}
	mask := updateRequest.GetUpdateMask()
	if mask == nil {
		return nil, serviceerror.NewInvalidArgument("UpdateMask is not provided")
	}

	updateFields := util.ParseFieldMask(mask)
	mergeInto := &activitypb.ActivityOptions{
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
	}

	// update activity options
	err := applyActivityOptions(mergeInto, mergeFrom, updateFields)
	if err != nil {
		return nil, err
	}

	// validate the updated options
	err = validateActivityOptions(validator, request.NamespaceId, ai, mergeInto)
	if err != nil {
		return nil, err
	}

	// update activity info with new options
	ai.TaskQueue = mergeInto.TaskQueue.Name
	ai.ScheduleToCloseTimeout = mergeInto.ScheduleToCloseTimeout
	ai.ScheduleToStartTimeout = mergeInto.ScheduleToStartTimeout
	ai.StartToCloseTimeout = mergeInto.StartToCloseTimeout
	ai.HeartbeatTimeout = mergeInto.HeartbeatTimeout
	ai.RetryMaximumInterval = mergeInto.RetryPolicy.MaximumInterval
	ai.RetryBackoffCoefficient = mergeInto.RetryPolicy.BackoffCoefficient
	ai.RetryMaximumInterval = mergeInto.RetryPolicy.MaximumInterval
	ai.RetryMaximumAttempts = mergeInto.RetryPolicy.MaximumAttempts

	// move forward activity version
	ai.Stamp++

	// invalidate timers
	ai.TimerTaskStatus = workflow.TimerTaskStatusNone

	// regenerate retry tasks
	if workflow.GetActivityState(ai) == enumspb.PENDING_ACTIVITY_STATE_SCHEDULED {
		// two options - it can be in backoff, or waiting to be started
		now := shardContext.GetTimeSource().Now()

		// if activity is past its scheduled time and ready to be started
		// we don't really need to do generate timer tasks, it should be done in closeTransaction
		if now.Before(ai.ScheduledTime.AsTime()) {
			// activity is in backoff
			_, err = mutableState.RetryActivity(ai, nil)
			if err != nil {
				return nil, err
			}
		}
	}

	// fill the response
	response.ActivityOptions = mergeInto

	return &api.UpdateWorkflowAction{
		Noop:               false,
		CreateWorkflowTask: false,
	}, nil
}

func applyActivityOptions(
	mergeInto *activitypb.ActivityOptions,
	mergeFrom *activitypb.ActivityOptions,
	updateFields map[string]struct{},
) error {

	if _, ok := updateFields["taskQueue.name"]; ok {
		if mergeFrom.TaskQueue == nil {
			return serviceerror.NewInvalidArgument("TaskQueue is not provided")
		}
		if mergeInto.TaskQueue == nil {
			mergeInto.TaskQueue = mergeFrom.TaskQueue
		}
		mergeInto.TaskQueue.Name = mergeFrom.TaskQueue.Name
	}

	if _, ok := updateFields["scheduleToCloseTimeout"]; ok {
		mergeInto.ScheduleToCloseTimeout = mergeFrom.ScheduleToCloseTimeout
	}

	if _, ok := updateFields["scheduleToStartTimeout"]; ok {
		mergeInto.ScheduleToStartTimeout = mergeFrom.ScheduleToStartTimeout
	}

	if _, ok := updateFields["startToCloseTimeout"]; ok {
		mergeInto.StartToCloseTimeout = mergeFrom.StartToCloseTimeout
	}

	if _, ok := updateFields["heartbeatTimeout"]; ok {
		mergeInto.HeartbeatTimeout = mergeFrom.HeartbeatTimeout
	}

	if mergeInto.RetryPolicy == nil {
		mergeInto.RetryPolicy = &commonpb.RetryPolicy{}
	}

	if _, ok := updateFields["retryPolicy.initialInterval"]; ok {
		if mergeFrom.RetryPolicy == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.RetryPolicy.InitialInterval = mergeFrom.RetryPolicy.InitialInterval
	}

	if _, ok := updateFields["retryPolicy.backoffCoefficient"]; ok {
		if mergeFrom.RetryPolicy == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.RetryPolicy.BackoffCoefficient = mergeFrom.RetryPolicy.BackoffCoefficient
	}

	if _, ok := updateFields["retryPolicy.maximumInterval"]; ok {
		if mergeFrom.RetryPolicy == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.RetryPolicy.MaximumInterval = mergeFrom.RetryPolicy.MaximumInterval
	}
	if _, ok := updateFields["retryPolicy.maximumAttempts"]; ok {
		if mergeFrom.RetryPolicy == nil {
			return serviceerror.NewInvalidArgument("RetryPolicy is not provided")
		}
		mergeInto.RetryPolicy.MaximumAttempts = mergeFrom.RetryPolicy.MaximumAttempts
	}

	return nil
}

func validateActivityOptions(
	validator *api.CommandAttrValidator,
	namespaceID string,
	ai *persistencespb.ActivityInfo,
	ao *activitypb.ActivityOptions,
) error {
	attributes := &commandpb.ScheduleActivityTaskCommandAttributes{
		TaskQueue:              ao.TaskQueue,
		ScheduleToCloseTimeout: ao.ScheduleToCloseTimeout,
		ScheduleToStartTimeout: ao.ScheduleToStartTimeout,
		StartToCloseTimeout:    ao.StartToCloseTimeout,
		HeartbeatTimeout:       ao.HeartbeatTimeout,
		ActivityId:             ai.ActivityId,
		ActivityType:           ai.ActivityType,
	}

	_, err := validator.ValidateActivityScheduleAttributes(namespace.ID(namespaceID), attributes, nil)
	return err
}
