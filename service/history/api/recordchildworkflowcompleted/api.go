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
	"errors"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

// maxResetRedirectCount is the number of times we follow the reset run ID to forward the request to the new parent.
// This redirection happens only when a workflow is reset and in most cases it's 1 or 2 hops.
// maxResetRedirectCount prevents us from following long chain of resets (or some circular loop in redirects).
const maxResetRedirectCount = 100

// This API records the child completion event in the parent's history. It does the following.
// - Rejects the request if the parent was closed for any reason other than reset.
// - If the parent was closed due to reset, it forwards the request to the new parent following the resetRunID link.
// - It ensures that the child sending the completion request was initialized by this parent before accepting the request.
func Invoke(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.RecordChildExecutionCompletedResponse, retError error) {
	_, err := api.GetActiveNamespace(shardContext, namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	// If the parent is reset, we need to follow a possible chain of resets to deliver the completion event to the correct parent.
	redirectCount := 0
	isForwarded := false
	for {
		resetRunID, err := recordChildWorkflowCompleted(ctx, request, shardContext, workflowConsistencyChecker, isForwarded)
		if errors.Is(err, consts.ErrWorkflowCompleted) {
			// if the parent was reset, forward the request to the new run pointed by resetRunID
			// Note: An alternative solution is to load the current run here and compare the originalRunIDs of the current run and the closed parent.
			// If they match, then deliver it to the current run. We should consider this optimization if we notice that reset chain is longer than 1-2 hops.
			if resetRunID != "" {
				if redirectCount >= maxResetRedirectCount {
					return nil, consts.ErrResetRedirectLimitReached
				}
				redirectCount++
				request.ParentExecution.RunId = resetRunID
				isForwarded = true
				continue
			}
		}
		if err != nil {
			return nil, err
		}
		return &historyservice.RecordChildExecutionCompletedResponse{}, nil
	}
}

// recordChildWorkflowCompleted records the child completed event in the parent history if the parent is still running.
// It returns consts.ErrWorkflowCompleted if the parent is already completed. Additionally a reset run ID is returned if the parent was completed due to a reset operation.
func recordChildWorkflowCompleted(
	ctx context.Context,
	request *historyservice.RecordChildExecutionCompletedRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	isForwarded bool,
) (string, error) {
	resetRunID := ""
	parentInitiatedID := request.ParentInitiatedId
	parentInitiatedVersion := request.ParentInitiatedVersion
	err := api.GetAndUpdateWorkflowWithConsistencyCheck(
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

			_, childInitEventFound := mutableState.GetChildExecutionInfo(parentInitiatedID)
			return childInitEventFound
		},
		definition.NewWorkflowKey(
			request.NamespaceId,
			request.GetParentExecution().WorkflowId,
			request.GetParentExecution().RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			if !mutableState.IsWorkflowExecutionRunning() {
				resetRunID = mutableState.GetExecutionInfo().ResetRunId
				return nil, consts.ErrWorkflowCompleted
			}

			onCurrentBranch, err := api.IsHistoryEventOnCurrentBranch(mutableState, parentInitiatedID, parentInitiatedVersion)
			if err != nil || !onCurrentBranch {
				return nil, consts.ErrChildExecutionNotFound
			}

			// Check mutable state to make sure child execution is in pending child executions
			ci, isRunning := mutableState.GetChildExecutionInfo(parentInitiatedID)
			if !isRunning {
				return nil, consts.ErrChildExecutionNotFound
			}

			// note we already checked if startedEventID is empty (in consistency predicate)
			// and reloaded mutable state, so if startedEventID is still missing, we need to
			// record a started event before recording completion event.
			if err := recordStartedEventIfMissing(ctx, mutableState, request, ci); err != nil {
				return nil, err
			}

			childExecution := request.GetChildExecution()
			if ci.GetStartedWorkflowId() != childExecution.GetWorkflowId() {
				// this can only happen when we don't have the initiated version
				return nil, consts.ErrChildExecutionNotFound
			}

			childrenInitializedAfterResetPoint := mutableState.GetChildrenInitializedPostResetPoint()
			if len(childrenInitializedAfterResetPoint) > 0 {
				// This parent was reset and it also has some children that potentially were restarted.
				initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, parentInitiatedID)
				if err != nil {
					return nil, consts.ErrChildExecutionNotFound
				}
				initiatedAttr := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
				childID := fmt.Sprintf("%s:%s", initiatedAttr.GetWorkflowType().Name, initiatedAttr.GetWorkflowId())
				_, ok := childrenInitializedAfterResetPoint[childID]
				if ok {
					// The child sending this request was restarted. Do not accept any forwarded completion events because the restarted child will directly send its completion event to this parent.
					// Sometimes the old child will race to complete before being restarted and it also happens to have the same initialized event ID. In such cases the result will come as forwarded request which we ignore here.
					if isForwarded {
						return nil, consts.ErrChildExecutionNotFound
					}
					// The results are from the child that this parent initiated. We should stop tracking the child and process the result now.
					delete(childrenInitializedAfterResetPoint, childID)
					mutableState.SetChildrenInitializedPostResetPoint(childrenInitializedAfterResetPoint)
				}
			}

			completionEvent := request.CompletionEvent
			switch completionEvent.GetEventType() {
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
				attributes := completionEvent.GetWorkflowExecutionCompletedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCompletedEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
				attributes := completionEvent.GetWorkflowExecutionFailedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionFailedEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
				attributes := completionEvent.GetWorkflowExecutionCanceledEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionCanceledEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
				attributes := completionEvent.GetWorkflowExecutionTerminatedEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTerminatedEvent(parentInitiatedID, childExecution, attributes)
			case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
				attributes := completionEvent.GetWorkflowExecutionTimedOutEventAttributes()
				_, err = mutableState.AddChildWorkflowExecutionTimedOutEvent(parentInitiatedID, childExecution, attributes)
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
		shardContext,
		workflowConsistencyChecker,
	)
	return resetRunID, err
}

func recordStartedEventIfMissing(
	ctx context.Context,
	mutableState workflow.MutableState,
	request *historyservice.RecordChildExecutionCompletedRequest,
	ci *persistencespb.ChildExecutionInfo,
) error {
	parentInitiatedID := request.ParentInitiatedId
	if ci.StartedEventId == common.EmptyEventID {
		initiatedEvent, err := mutableState.GetChildExecutionInitiatedEvent(ctx, parentInitiatedID)
		if err != nil {
			return consts.ErrChildExecutionNotFound
		}
		initiatedAttr := initiatedEvent.GetStartChildWorkflowExecutionInitiatedEventAttributes()
		// note values used here should not matter because the child info will be deleted
		// when the response is recorded, so it should be fine e.g. that ci.Clock is nil
		_, err = mutableState.AddChildWorkflowExecutionStartedEvent(
			request.GetChildExecution(),
			initiatedAttr.WorkflowType,
			initiatedEvent.EventId,
			initiatedAttr.Header,
			ci.Clock,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
