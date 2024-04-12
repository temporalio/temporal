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

package isworkflowtaskvalid

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	req *historyservice.IsWorkflowTaskValidRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.IsWorkflowTaskValidResponse, retError error) {
	isValid := false
	err := api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.Execution.WorkflowId,
			req.Execution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			isTaskValid, err := isWorkflowTaskValid(workflowLease, req.ScheduledEventId)
			if err != nil {
				return nil, err
			}
			isValid = isTaskValid
			return &api.UpdateWorkflowAction{
				Noop:               true,
				CreateWorkflowTask: false,
			}, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)
	return &historyservice.IsWorkflowTaskValidResponse{
		IsValid: isValid,
	}, err
}

func isWorkflowTaskValid(
	workflowLease api.WorkflowLease,
	scheduledEventID int64,
) (bool, error) {
	mutableState := workflowLease.GetMutableState()
	if !mutableState.IsWorkflowExecutionRunning() {
		return false, consts.ErrWorkflowCompleted
	}

	workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)
	if workflowTask == nil {
		return false, nil
	}
	return workflowTask.StartedEventID == common.EmptyEventID, nil
}
