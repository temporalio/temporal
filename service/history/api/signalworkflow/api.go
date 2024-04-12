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

package signalworkflow

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	req *historyservice.SignalWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.SignalWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := req.SignalRequest
	externalWorkflowExecution := req.ExternalWorkflowExecution
	childWorkflowOnly := req.GetChildWorkflowOnly()

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			namespaceID.String(),
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if request.GetRequestId() != "" && mutableState.IsSignalRequested(request.GetRequestId()) {
				return &api.UpdateWorkflowAction{
					Noop:               true,
					CreateWorkflowTask: false,
				}, nil
			}

			releaseFn := workflowLease.GetReleaseFn()
			if !mutableState.IsWorkflowExecutionRunning() {
				// in-memory mutable state is still clean, release the lock with nil error to prevent
				// clearing and reloading mutable state
				releaseFn(nil)
				return nil, consts.ErrWorkflowCompleted
			}

			if err := api.ValidateSignal(
				ctx,
				shard,
				mutableState,
				request.GetInput().Size(),
				"SignalWorkflowExecution",
			); err != nil {
				releaseFn(nil)
				return nil, err
			}

			executionInfo := mutableState.GetExecutionInfo()

			// Do not create workflow task when the workflow has first workflow task backoff and execution is not started yet
			createWorkflowTask := !mutableState.IsWorkflowPendingOnWorkflowTaskBackoff() && !request.GetSkipGenerateWorkflowTask()

			if childWorkflowOnly {
				parentWorkflowID := executionInfo.ParentWorkflowId
				parentRunID := executionInfo.ParentRunId
				if externalWorkflowExecution.GetWorkflowId() != parentWorkflowID ||
					externalWorkflowExecution.GetRunId() != parentRunID {
					releaseFn(nil)
					return nil, consts.ErrWorkflowParent
				}
			}

			if request.GetRequestId() != "" {
				mutableState.AddSignalRequested(request.GetRequestId())
			}
			_, err := mutableState.AddWorkflowExecutionSignaledEvent(
				request.GetSignalName(),
				request.GetInput(),
				request.GetIdentity(),
				request.GetHeader(),
				request.GetSkipGenerateWorkflowTask(),
				externalWorkflowExecution,
			)
			if err != nil {
				return nil, err
			}

			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: createWorkflowTask,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.SignalWorkflowExecutionResponse{}, nil
}
