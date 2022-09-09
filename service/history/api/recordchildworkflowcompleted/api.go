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

package recordchildworkflowcompleted

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	_, err := api.GetActiveNamespace(shard, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	parentInitiatedID := request.ParentInitiatedId
	parentInitiatedVersion := request.ParentInitiatedVersion

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		request.Clock,
		func(mutableState workflow.MutableState) bool {
			if !mutableState.IsWorkflowExecutionRunning() {
				// current branch already closed, we won't perform any operation, pass the check
				return true
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil {
				// can't find initiated event, potential stale mutable, fail the predicate check
				return false
			}
			if !onCurrentBranch {
				// found on different branch, since we don't record completion on a different branch, pass the check
				return true
			}

			ci, isRunning := mutableState.GetChildExecutionInfo(parentInitiatedID)
			return !(isRunning && ci.StartedEventId == common.EmptyEventID) // !(potential stale)
		},
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.WorkflowExecution.WorkflowId,
			request.WorkflowExecution.RunId,
		),
		func(workflowContext api.WorkflowContext) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowContext.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil || !onCurrentBranch {
				return nil, consts.ErrChildExecutionNotFound
			}

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(parentInitiatedID)
			if !isRunning || ci.StartedEventId == common.EmptyEventID {
				// note we already checked if startedEventID is empty (in consistency predicate)
				// and reloaded mutable state
				return nil, consts.ErrChildExecutionNotFound
			}

			completedExecution := request.CompletedExecution
			if ci.GetStartedWorkflowId() != completedExecution.GetWorkflowId() {
				// this can only happen when we don't have the initiated version
				return nil, consts.ErrChildExecutionNotFound
			}

			completionEvent := request.CompletionEvent
			switch completionEvent.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				attributes := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(parentInitiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				attributes := completionEvent.GetWorkflowExecutionFailedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionFailedEvent(parentInitiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				attributes := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(parentInitiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
				attributes := completionEvent.GetWorkflowExecutionTerminatedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(parentInitiatedID, completedExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
				attributes := completionEvent.GetWorkflowExecutionTimedOutEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(parentInitiatedID, completedExecution, attributes)
			}

			if err != nil {
				return nil, err
			}
			return &api.UpdateWorkflowAction{
				Noop:               false,
				CreateWorkflowTask: true,
			}, nil
		},
		nil,
		shard,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.RecordChildExecutionCompletedResponse{}, nil
}
