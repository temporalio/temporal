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

package resetactivity

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	req *historyservice.ResetActivityRequest,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.ResetActivityResponse, retError error) {
	request := req.GetFrontendRequest()
	workflowKey := definition.NewWorkflowKey(
		req.NamespaceId,
		request.GetExecution().GetWorkflowId(),
		request.GetExecution().GetRunId(),
	)

	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		workflowKey,
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			var activityIDs []string
			switch a := request.GetActivity().(type) {
			case *workflowservice.ResetActivityRequest_Id:
				activityIDs = append(activityIDs, a.Id)
			case *workflowservice.ResetActivityRequest_Type:
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
				if err := workflow.ResetActivity(
					shardContext, mutableState, activityId,
					request.ResetHeartbeat, request.KeepPaused, request.Jitter.AsDuration(),
				); err != nil {
					return nil, err
				}
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

	return &historyservice.ResetActivityResponse{}, nil
}
