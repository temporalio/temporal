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
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
)

const longPollSoftTimeout = time.Second

//nolint:revive // cognitive complexity 39 (> max enabled 25)
func GetOrPollWorkflowMutableState(
	ctx context.Context,
	shardContext historyi.ShardContext,
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

	if len(request.GetExecution().GetRunId()) == 0 {
		runId, err := workflowConsistencyChecker.GetCurrentWorkflowRunID(
			ctx,
			request.GetNamespaceId(),
			request.GetExecution().GetWorkflowId(),
			locks.PriorityHigh,
		)
		if err != nil {
			return nil, err
		}
		request.GetExecution().SetRunId(runId)
	}
	workflowKey := definition.NewWorkflowKey(
		request.GetNamespaceId(),
		request.GetExecution().GetWorkflowId(),
		request.GetExecution().GetRunId(),
	)
	response, err := GetMutableStateWithConsistencyCheck(
		ctx,
		shardContext,
		workflowKey,
		request.GetVersionHistoryItem().GetVersion(),
		request.GetVersionHistoryItem().GetEventId(),
		request.GetVersionedTransition(),
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
		request.SetVersionHistoryItem(lastVersionHistoryItem)
	}

	transitionHistory := response.GetTransitionHistory()
	currentVersionedTransition := transitionhistory.LastVersionedTransition(transitionHistory)
	if len(transitionHistory) != 0 && request.HasVersionedTransition() {
		if transitionhistory.StalenessCheck(transitionHistory, request.GetVersionedTransition()) != nil {
			logger.Warn(fmt.Sprintf("Request versioned transition and transition history don't match. Request: %v, current: %v",
				request.GetVersionedTransition(),
				currentVersionedTransition),
				tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
				tag.WorkflowID(workflowKey.GetWorkflowID()),
				tag.WorkflowRunID(workflowKey.GetRunID()))
			return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(),
				request.GetCurrentBranchToken(),
				currentVersionedTransition,
				request.GetVersionedTransition())
		}
	}

	// Use the latest event id + event version as the branch identifier. This pair is unique across clusters.
	// We return the full version histories. Callers need to fetch the last version history item from current branch
	// and use the last version history item in following calls.
	if !versionhistory.ContainsVersionHistoryItem(currentVersionHistory, request.GetVersionHistoryItem()) {
		logItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, err
		}
		logger.Warn("Request history branch and current history branch don't match",
			tag.Value(logItem),
			tag.TokenLastEventVersion(request.GetVersionHistoryItem().GetVersion()),
			tag.TokenLastEventID(request.GetVersionHistoryItem().GetEventId()),
			tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
			tag.WorkflowID(workflowKey.GetWorkflowID()),
			tag.WorkflowRunID(workflowKey.GetRunID()))
		return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(),
			request.GetCurrentBranchToken(),
			currentVersionedTransition,
			request.GetVersionedTransition())
	}

	// expectedNextEventID is 0 when caller want to get the current next event ID without blocking.
	expectedNextEventID := common.FirstEventID
	if request.GetExpectedNextEventId() != common.EmptyEventID {
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
			request.GetVersionHistoryItem().GetVersion(),
			request.GetVersionHistoryItem().GetEventId(),
			request.GetVersionedTransition(),
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
		currentVersionedTransition := transitionhistory.LastVersionedTransition(transitionHistory)
		if len(transitionHistory) != 0 && request.HasVersionedTransition() {
			if transitionhistory.StalenessCheck(transitionHistory, request.GetVersionedTransition()) != nil {
				logger.Warn(fmt.Sprintf("Request versioned transition and transition history don't match prior to polling the mutable state. Request: %v, current: %v",
					request.GetVersionedTransition(),
					currentVersionedTransition),
					tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
					tag.WorkflowID(workflowKey.GetWorkflowID()),
					tag.WorkflowRunID(workflowKey.GetRunID()))
				return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(), request.GetCurrentBranchToken(), currentVersionedTransition, request.GetVersionedTransition())
			}
		}
		if !versionhistory.ContainsVersionHistoryItem(currentVersionHistory, request.GetVersionHistoryItem()) {
			logItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
			if err != nil {
				return nil, err
			}
			logger.Warn("Request history branch and current history branch don't match prior to polling the mutable state",
				tag.Value(logItem),
				tag.TokenLastEventVersion(request.GetVersionHistoryItem().GetVersion()),
				tag.TokenLastEventID(request.GetVersionHistoryItem().GetEventId()),
				tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
				tag.WorkflowID(workflowKey.GetWorkflowID()),
				tag.WorkflowRunID(workflowKey.GetRunID()))
			return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(), request.GetCurrentBranchToken(), currentVersionedTransition, request.GetVersionedTransition())
		}
		if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return response, nil
		}

		namespaceRegistry, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespaceID)
		if err != nil {
			return nil, err
		}

		// Send back response just before caller context would time out.
		longPollInterval := shardContext.GetConfig().LongPollExpirationInterval(namespaceRegistry.Name().String())
		longPollCtx, cancel := contextutil.WithDeadlineBuffer(ctx, longPollInterval, longPollSoftTimeout)
		defer cancel()

		for {
			select {
			case event := <-channel:
				response.SetLastFirstEventId(event.LastFirstEventID)
				response.SetLastFirstEventTxnId(event.LastFirstEventTxnID)
				response.SetNextEventId(event.NextEventID)
				response.SetPreviousStartedEventId(event.PreviousStartedEventID)
				response.SetWorkflowState(event.WorkflowState)
				response.SetWorkflowStatus(event.WorkflowStatus)
				// Note: Later events could modify response.WorkerVersionStamp and we won't
				// update it here. That's okay since this return value is only informative and isn't used for task dispatch.
				// For correctness we could pass it in the Notification event.
				eventVersionHistory, err := versionhistory.GetCurrentVersionHistory(event.VersionHistories)
				if err != nil {
					return nil, err
				}
				response.SetCurrentBranchToken(eventVersionHistory.GetBranchToken())
				response.SetVersionHistories(event.VersionHistories)
				response.SetTransitionHistory(event.TransitionHistory)

				notifiedEventVersionItem, err := versionhistory.GetLastVersionHistoryItem(eventVersionHistory)
				if err != nil {
					return nil, err
				}
				// It is possible the notifier sends an out of date event, we can ignore this event.
				if versionhistory.CompareVersionHistoryItem(notifiedEventVersionItem, request.GetVersionHistoryItem()) < 0 {
					continue
				}
				transitionHistory := response.GetTransitionHistory()
				currentVersionedTransition := transitionhistory.LastVersionedTransition(transitionHistory)
				if len(transitionHistory) != 0 && request.HasVersionedTransition() {
					if transitionhistory.StalenessCheck(transitionHistory, request.GetVersionedTransition()) != nil {
						logger.Warn(fmt.Sprintf("Request versioned transition and transition history don't match after polling the mutable state. Request: %v, current: %v",
							request.GetVersionedTransition(),
							currentVersionedTransition),
							tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
							tag.WorkflowID(workflowKey.GetWorkflowID()),
							tag.WorkflowRunID(workflowKey.GetRunID()))
						return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(), request.GetCurrentBranchToken(), currentVersionedTransition, request.GetVersionedTransition())
					}
				}
				if !versionhistory.ContainsVersionHistoryItem(eventVersionHistory, request.GetVersionHistoryItem()) {
					logger.Warn("Request history branch and current history branch don't match after polling the mutable state",
						tag.Value(notifiedEventVersionItem),
						tag.TokenLastEventVersion(request.GetVersionHistoryItem().GetVersion()),
						tag.TokenLastEventID(request.GetVersionHistoryItem().GetEventId()),
						tag.WorkflowNamespaceID(workflowKey.GetNamespaceID()),
						tag.WorkflowID(workflowKey.GetWorkflowID()),
						tag.WorkflowRunID(workflowKey.GetRunID()))
					return nil, serviceerrors.NewCurrentBranchChanged(response.GetCurrentBranchToken(), request.GetCurrentBranchToken(), currentVersionedTransition, request.GetVersionedTransition())
				}
				if expectedNextEventID < response.GetNextEventId() || response.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
					return response, nil
				}
			case <-longPollCtx.Done():
				return response, nil
			case <-ctx.Done():
				// Fallback for when ctx.Deadline() returns false but ctx is still cancelled.
				// This can happen when gRPC timeout header isn't propagated (e.g., stripped
				// by proxy) but the client still disconnects/cancels when its timeout fires.
				// In normal operation where ctx.Deadline() returns true, longPollCtx.Done()
				// fires first and this case is never reached.
				return response, nil
			}
		}
	}

	return response, nil
}

func GetMutableState(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternalf(
			"getMutableState encountered empty run ID: %v", workflowKey,
		)
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
	shardContext historyi.ShardContext,
	workflowKey definition.WorkflowKey,
	currentVersion int64,
	currentEventID int64,
	versionedTransition *persistencespb.VersionedTransition,
	workflowConsistencyChecker WorkflowConsistencyChecker,
) (_ *historyservice.GetMutableStateResponse, retError error) {

	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternalf(
			"getMutableState encountered empty run ID: %v", workflowKey,
		)
	}

	workflowLease, err := workflowConsistencyChecker.GetWorkflowLeaseWithConsistencyCheck(
		ctx,
		nil,
		func(mutableState historyi.MutableState) bool {
			transitionHistory := mutableState.GetExecutionInfo().GetTransitionHistory()
			if len(transitionHistory) != 0 && versionedTransition != nil {
				return transitionhistory.StalenessCheck(transitionHistory, versionedTransition) == nil
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
	mutableState historyi.MutableState,
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
		mostRecentWorkerVersionStamp = commonpb.WorkerVersionStamp_builder{
			BuildId:       mrwvs.GetBuildId(),
			UseVersioning: mrwvs.GetUseVersioning(),
		}.Build()
	}

	return historyservice.GetMutableStateResponse_builder{
		Execution: commonpb.WorkflowExecution_builder{
			WorkflowId: mutableState.GetExecutionInfo().GetWorkflowId(),
			RunId:      mutableState.GetExecutionState().GetRunId(),
		}.Build(),
		WorkflowType:           commonpb.WorkflowType_builder{Name: executionInfo.GetWorkflowTypeName()}.Build(),
		LastFirstEventId:       lastFirstEventID,
		LastFirstEventTxnId:    lastFirstEventTxnID,
		NextEventId:            mutableState.GetNextEventID(),
		PreviousStartedEventId: mutableState.GetLastCompletedWorkflowTaskStartedEventId(),
		TaskQueue: taskqueuepb.TaskQueue_builder{
			Name: executionInfo.GetTaskQueue(),
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
		}.Build(),
		StickyTaskQueue: taskqueuepb.TaskQueue_builder{
			Name:       executionInfo.GetStickyTaskQueue(),
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			NormalName: executionInfo.GetTaskQueue(),
		}.Build(),
		StickyTaskQueueScheduleToStartTimeout: executionInfo.GetStickyScheduleToStartTimeout(),
		CurrentBranchToken:                    currentBranchToken,
		WorkflowState:                         workflowState,
		WorkflowStatus:                        workflowStatus,
		IsStickyTaskQueueEnabled:              mutableState.IsStickyTaskQueueSet(),
		VersionHistories: versionhistory.CopyVersionHistories(
			mutableState.GetExecutionInfo().GetVersionHistories(),
		),
		FirstExecutionRunId:          executionInfo.GetFirstExecutionRunId(),
		AssignedBuildId:              mutableState.GetAssignedBuildId(),
		InheritedBuildId:             mutableState.GetInheritedBuildId(),
		MostRecentWorkerVersionStamp: mostRecentWorkerVersionStamp,
		TransitionHistory:            transitionhistory.CopyVersionedTransitions(mutableState.GetExecutionInfo().GetTransitionHistory()),
		VersioningInfo:               mutableState.GetExecutionInfo().GetVersioningInfo(),
	}.Build(), nil
}
