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
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func GetOrPollMutableState(
	ctx context.Context,
	shardContext shard.Context,
	request *historyservice.GetMutableStateRequest,
	workflowConsistencyChecker WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
) (*historyservice.GetMutableStateResponse, error) {

	logger := shardContext.GetLogger()
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
			locks.PriorityHigh,
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
	response, err := GetMutableStateWithConsistencyCheck(
		ctx,
		shardContext,
		workflowKey,
		request.VersionHistoryItem.GetVersion(),
		request.VersionHistoryItem.GetEventId(),
		request.VersionedTransition,
		workflowConsistencyChecker,
	)
	if err != nil {
		return nil, err
	}
	currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
	if err != nil {
		return nil, err
	}
	if request.GetVersionHistoryItem() == nil {
		lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, err
		}
		request.VersionHistoryItem = lastVersionHistoryItem
	}
	transitionHistory := response.GetTransitionHistory()
	if len(transitionHistory) != 0 {
		if request.VersionedTransition != nil {
			if workflow.TransitionHistoryStalenessCheck(transitionHistory, request.VersionedTransition) != nil {
				logger.Warn(fmt.Sprintf("Request versioned transition and transition history don't match. Request: %v, current: %v",
					request.VersionedTransition,
					transitionHistory[len(transitionHistory)-1]),
					tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
					tag.WorkflowID(workflowKey.GetWorkflowID()),
					tag.WorkflowRunID(workflowKey.GetRunID()))
				return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
			}
		}
	} else {
		// Use the latest event id + event version as the branch identifier. This pair is unique across clusters.
		// We return the full version histories. Callers need to fetch the last version history item from current branch
		// and use the last version history item in following calls.
		if !versionhistory.ContainsVersionHistoryItem(currentVersionHistory, request.VersionHistoryItem) {
			logItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
			if err != nil {
				return nil, err
			}
			logger.Warn("Request history branch and current history branch don't match",
				tag.Value(logItem),
				tag.TokenLastEventVersion(request.VersionHistoryItem.GetVersion()),
				tag.TokenLastEventID(request.VersionHistoryItem.GetEventId()),
				tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
				tag.WorkflowID(workflowKey.GetWorkflowID()),
				tag.WorkflowRunID(workflowKey.GetRunID()))
			return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
		}
	}

	// expectedNextEventID is 0 when caller want to get the current next event ID without blocking.
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
		response, err = GetMutableStateWithConsistencyCheck(
			ctx,
			shardContext,
			workflowKey,
			request.VersionHistoryItem.GetVersion(),
			request.VersionHistoryItem.GetEventId(),
			request.VersionedTransition,
			workflowConsistencyChecker,
		)
		if err != nil {
			return nil, err
		}
		currentVersionHistory, err = versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
		if err != nil {
			return nil, err
		}

		transitionHistory := response.GetTransitionHistory()
		if len(transitionHistory) != 0 {
			if request.VersionedTransition != nil && workflow.TransitionHistoryStalenessCheck(transitionHistory, request.VersionedTransition) != nil {
				logger.Warn(fmt.Sprintf("Request versioned transition and transition history prior to polling the mutable state. Request: %v, current: %v",
					request.VersionedTransition,
					transitionHistory[len(transitionHistory)-1]),
					tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
					tag.WorkflowID(workflowKey.GetWorkflowID()),
					tag.WorkflowRunID(workflowKey.GetRunID()))
				return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
			}
		} else if !versionhistory.ContainsVersionHistoryItem(currentVersionHistory, request.VersionHistoryItem) {
			logItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
			if err != nil {
				return nil, err
			}
			logger.Warn("Request history branch and current history branch don't match prior to polling the mutable state",
				tag.Value(logItem),
				tag.TokenLastEventVersion(request.VersionHistoryItem.GetVersion()),
				tag.TokenLastEventID(request.VersionHistoryItem.GetEventId()),
				tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
				tag.WorkflowID(workflowKey.GetWorkflowID()),
				tag.WorkflowRunID(workflowKey.GetRunID()))
			return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
		}
		if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return response, nil
		}

		namespaceRegistry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
		if err != nil {
			return nil, err
		}
		timer := time.NewTimer(shardContext.GetConfig().LongPollExpirationInterval(namespaceRegistry.Name().String()))
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
				eventVersionHistory, err := versionhistory.GetCurrentVersionHistory(event.VersionHistories)
				if err != nil {
					return nil, err
				}
				response.CurrentBranchToken = eventVersionHistory.GetBranchToken()
				response.VersionHistories = event.VersionHistories
				response.TransitionHistory = event.TransitionHistory

				notifiedEventVersionItem, err := versionhistory.GetLastVersionHistoryItem(eventVersionHistory)
				if err != nil {
					return nil, err
				}
				// It is possible the notifier sends an out of date event, we can ignore this event.
				if versionhistory.CompareVersionHistoryItem(notifiedEventVersionItem, request.VersionHistoryItem) < 0 {
					continue
				}
				transitionHistory := response.GetTransitionHistory()
				if len(transitionHistory) != 0 {
					if request.VersionedTransition != nil && workflow.TransitionHistoryStalenessCheck(transitionHistory, request.VersionedTransition) != nil {
						logger.Warn(fmt.Sprintf("Request versioned transition and transition history don't match after polling the mutable state. Request: %v, current: %v",
							request.VersionedTransition,
							transitionHistory[len(transitionHistory)-1]),
							tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
							tag.WorkflowID(workflowKey.GetWorkflowID()),
							tag.WorkflowRunID(workflowKey.GetRunID()))
						return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
					}
				} else if !versionhistory.ContainsVersionHistoryItem(eventVersionHistory, request.VersionHistoryItem) {
					logger.Warn("Request history branch and current history branch don't match after polling the mutable state",
						tag.Value(notifiedEventVersionItem),
						tag.TokenLastEventVersion(request.VersionHistoryItem.GetVersion()),
						tag.TokenLastEventID(request.VersionHistoryItem.GetEventId()),
						tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
						tag.WorkflowID(workflowKey.GetWorkflowID()),
						tag.WorkflowRunID(workflowKey.GetRunID()))
					return nil, serviceerrors.NewCurrentBranchChanged(response.CurrentBranchToken, request.CurrentBranchToken)
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
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"getMutableState encountered empty run ID: %v", workflowKey,
		))
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState, err := workflowLease.GetContext().LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}
	return MutableStateToGetResponse(mutableState)
}

func GetMutableStateWithConsistencyCheck(
	ctx context.Context,
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	currentVersion int64,
	currentEventID int64,
	versionedTransition *persistencespb.VersionedTransition,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"getMutableState encountered empty run ID: %v", workflowKey,
		))
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState workflow.MutableState) bool {
			transitionHistory := mutableState.GetExecutionInfo().GetTransitionHistory()
			if len(transitionHistory) != 0 {
				if versionedTransition != nil && workflow.TransitionHistoryStalenessCheck(transitionHistory, versionedTransition) != nil {
					return false
				}
				return true
			}

			currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(mutableState.GetExecutionInfo().GetVersionHistories())
			if err != nil {
				return false
			}
			lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
			if err != nil {
				return false
			}

			if currentVersion == lastVersionHistoryItem.GetVersion() {
				return currentEventID <= lastVersionHistoryItem.GetEventId()
			}
			return currentVersion < lastVersionHistoryItem.GetVersion()
		},
		workflowKey,
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { workflowLease.GetReleaseFn()(retError) }()

	mutableState, err := workflowLease.GetContext().LoadMutableState(ctx, shardContext)
	if err != nil {
		return nil, err
	}
	return MutableStateToGetResponse(mutableState)
}

func MutableStateToGetResponse(
	mutableState workflow.MutableState,
) (*historyservice.GetMutableStateResponse, error) {
	// NOTE: fields of GetMutableStateResponse (returned value of this func)
	// are accessed outside of workflow lock, and, therefore,
	// ***MUST*** be copied by value from mutableState fields.
	// strings are immutable, []byte is also considered to be immutable.

	currentBranchToken, err := mutableState.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}

	executionInfo := mutableState.GetExecutionInfo()
	workflowState, workflowStatus := mutableState.GetWorkflowStateStatus()
	lastFirstEventID, lastFirstEventTxnID := mutableState.GetLastFirstEventIDTxnID()

	var mostRecentWorkerVersionStamp *commonpb.WorkerVersionStamp
	if mrwvs := mutableState.GetExecutionInfo().GetMostRecentWorkerVersionStamp(); mrwvs != nil {
		mostRecentWorkerVersionStamp = &commonpb.WorkerVersionStamp{
			BuildId:       mrwvs.GetBuildId(),
			UseVersioning: mrwvs.GetUseVersioning(),
		}
	}

	return &historyservice.GetMutableStateResponse{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: mutableState.GetExecutionInfo().WorkflowId,
			RunId:      mutableState.GetExecutionState().RunId,
		},
		WorkflowType:           &commonpb.WorkflowType{Name: executionInfo.WorkflowTypeName},
		LastFirstEventId:       lastFirstEventID,
		LastFirstEventTxnId:    lastFirstEventTxnID,
		NextEventId:            mutableState.GetNextEventID(),
		PreviousStartedEventId: mutableState.GetLastCompletedWorkflowTaskStartedEventId(),
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
		FirstExecutionRunId:          executionInfo.FirstExecutionRunId,
		AssignedBuildId:              mutableState.GetAssignedBuildId(),
		InheritedBuildId:             mutableState.GetInheritedBuildId(),
		MostRecentWorkerVersionStamp: mostRecentWorkerVersionStamp,
		TransitionHistory:            mutableState.GetExecutionInfo().TransitionHistory,
		VersioningInfo:               mutableState.GetExecutionInfo().VersioningInfo,
	}, nil
}
