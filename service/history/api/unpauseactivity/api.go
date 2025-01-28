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

package unpauseactivity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.UnpauseActivityRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.UnpauseActivityResponse, retError error) {
	var response *historyservice.UnpauseActivityResponse

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetFrontendRequest().WorkflowId,
			request.GetFrontendRequest().RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			var err error
			response, err = processUnpauseActivityRequest(shardContext, mutableState, request)
			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: false,
			}, nil
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

func processUnpauseActivityRequest(
	shardContext shard.Context,
	mutableState workflow.MutableState,
	request *historyservice.UnpauseActivityRequest,
) (*historyservice.UnpauseActivityResponse, error) {

	if !mutableState.IsWorkflowExecutionRunning() {
		return nil, consts.ErrWorkflowCompleted
	}
	frontendRequest := request.GetFrontendRequest()
	var activityIDs []string
	switch a := frontendRequest.GetActivity().(type) {
	case *workflowservice.UnpauseActivityByIdRequest_Id:
		activityIDs = append(activityIDs, a.Id)
	case *workflowservice.UnpauseActivityByIdRequest_Type:
		activityType := a.Type
		for _, ai := range mutableState.GetPendingActivityInfos() {
			if ai.ActivityType.Name == activityType {
				activityIDs = append(activityIDs, ai.ActivityId)
			}
		}
	}

	if len(activityIDs) == 0 {
		return nil, consts.ErrActivityNotFound
	}

	for _, activityId := range activityIDs {

		ai, activityFound := mutableState.GetActivityByActivityID(activityId)

		if !activityFound {
			return nil, consts.ErrActivityNotFound
		}

		if !ai.Paused {
			// do nothing
			continue
		}

		if err := workflow.UnpauseActivity(
			shardContext, mutableState, ai,
			frontendRequest.GetResetAttempts(),
			frontendRequest.GetResetHeartbeat(),
			frontendRequest.GetJitter().AsDuration()); err != nil {
			return nil, err
		}

	}

	return &historyservice.UnpauseActivityResponse{}, nil
}
