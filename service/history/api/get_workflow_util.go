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

package api

import (
	"bytes"
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func GetOrPollMutableState(
	ctx context.Context,
	request *historyservice.GetMutableStateRequest,
	shard shard.Context,
	workflowConsistencyChecker WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
) (*historyservice.GetMutableStateResponse, error) {

	namespaceID := namespace.ID(request.GetNamespaceId())
	err := ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	if len(request.Execution.RunId) == 0 {
		request.Execution.RunId, err = workflowConsistencyChecker.GetCurrentRunID(
			ctx,
			request.NamespaceId,
			request.Execution.WorkflowId,
		)
		if err != nil {
			return nil, err
		}
	}
	workflowKey := definition.NewWorkflowKey(
		request.NamespaceId,
		request.Execution.WorkflowId,
		request.Execution.RunId,
	)
	response, err := GetMutableState(ctx, workflowKey, workflowConsistencyChecker)
	if err != nil {
		return nil, err
	}
	if request.CurrentBranchToken == nil {
		request.CurrentBranchToken = response.CurrentBranchToken
	}
	if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
		return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
	}

	// expectedNextEventID is 0 when caller want to get the current next event ID without blocking
	expectedNextEventID := common.FirstEventID
	if request.ExpectedNextEventId != common.EmptyEventID {
		expectedNextEventID = request.GetExpectedNextEventId()
	}

	// if caller decide to long poll on workflow execution
	// and the event ID we are looking for is smaller than current next event ID
	if expectedNextEventID >= response.GetNextEventId() && response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
		subscriberID, channel, err := eventNotifier.WatchHistoryEvent(workflowKey)
		if err != nil {
			return nil, err
		}
		defer func() { _ = eventNotifier.UnwatchHistoryEvent(workflowKey, subscriberID) }()
		// check again in case the next event ID is updated
		response, err = GetMutableState(ctx, workflowKey, workflowConsistencyChecker)
		if err != nil {
			return nil, err
		}
		// check again if the current branch token changed
		if !bytes.Equal(request.CurrentBranchToken, response.CurrentBranchToken) {
			return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
		}
		if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return response, nil
		}

		namespaceRegistry, err := shard.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(shard.GetConfig().LongPollExpirationInterval(namespaceRegistry.Name().String()))
		defer timer.Stop()
		for {
			select {
			case event := <-channel:
				response.LastFirstEventId = event.LastFirstEventID
				response.LastFirstEventTxnId = event.LastFirstEventTxnID
				response.NextEventId = event.NextEventID
				response.PreviousStartedEventId = event.PreviousStartedEventID
				response.WorkflowState = event.WorkflowState
				response.WorkflowStatus = event.WorkflowStatus
				// Note: Later events could modify response.WorkerVersionStamp and we won't
				// update it here. That's okay since this return value is only informative and isn't used for task dispatch.
				// For correctness we could pass it in the Notification event.
				if !bytes.Equal(request.CurrentBranchToken, event.CurrentBranchToken) {
					return nil, serviceerrors.NewCurrentBranchChanged(event.CurrentBranchToken, request.CurrentBranchToken)
				}
				if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
					return response, nil
				}
			case <-timer.C:
				return response, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}

	return response, nil
}

func GetMutableState(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"getMutableState encountered empty run ID: %v", workflowKey,
		))
	}

	weCtx, err := workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		BypassMutableStateConsistencyPredicate,
		workflowKey,
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { weCtx.GetReleaseFn()(retError) }()

	mutableState, err := weCtx.GetContext().LoadMutableState(ctx)
	if err != nil {
		return nil, err
	}
	return MutableStateToGetResponse(mutableState)
}

func MutableStateToGetResponse(
	mutableState workflow.MutableState,
) (*historyservice.GetMutableStateResponse, error) {
	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowState, workflowStatus := mutableState.GetWorkflowStateStatus()
	lastFirstEventID, lastFirstEventTxnID := mutableState.GetLastFirstEventIDTxnID()
	return &historyservice.GetMutableStateResponse{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: mutableState.GetExecutionInfo().WorkflowId,
			RunId:      mutableState.GetExecutionState().RunId,
		},
		WorkflowType:           &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
		LastFirstEventId:       lastFirstEventID,
		LastFirstEventTxnId:    lastFirstEventTxnID,
		NextEventId:            mutableState.GetNextEventID(),
		PreviousStartedEventId: mutableState.GetLastWorkflowTaskStartedEventID(),
		TaskQueue: &taskqueuepb.TaskQueue{
			Name: executionInfo.TaskQueue,
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		},
		StickyTaskQueue: &taskqueuepb.TaskQueue{
			Name:       executionInfo.StickyTaskQueue,
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: executionInfo.TaskQueue,
		},
		StickyTaskQueueScheduleToStartTimeout: executionInfo.StickyScheduleToStartTimeout,
		CurrentBranchToken:                    currentBranchToken,
		WorkflowState:                         workflowState,
		WorkflowStatus:                        workflowStatus,
		IsStickyTaskQueueEnabled:              mutableState.IsStickyTaskQueueSet(),
		VersionHistories: versionhistory.CopyVersionHistories(
			mutableState.GetExecutionInfo().GetVersionHistories(),
		),
		FirstExecutionRunId: executionInfo.FirstExecutionRunId,
		WorkerVersionStamp:  executionInfo.WorkerVersionStamp,
	}, nil
}
