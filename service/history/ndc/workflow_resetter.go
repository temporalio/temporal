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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination workflow_resetter_mock.go

package ndc

import (
	"context"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	workflowResetReapplyEventsFn func(ctx context.Context, resetMutableState workflow.MutableState) error

	WorkflowResetter interface {
		ResetWorkflow(
			ctx context.Context,
			namespaceID namespace.ID,
			workflowID string,
			baseRunID string,
			baseBranchToken []byte,
			baseRebuildLastEventID int64,
			baseRebuildLastEventVersion int64,
			baseNextEventID int64,
			resetRunID string,
			resetRequestID string,
			currentWorkflow Workflow,
			resetReason string,
			additionalReapplyEvents []*historypb.HistoryEvent,
			resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
		) error
	}

	stateRebuilderProvider func() StateRebuilder

	workflowResetterImpl struct {
		shardContext      shard.Context
		namespaceRegistry namespace.Registry
		clusterMetadata   cluster.Metadata
		executionMgr      persistence.ExecutionManager
		workflowCache     wcache.Cache
		newStateRebuilder stateRebuilderProvider
		transaction       workflow.Transaction
		logger            log.Logger
	}
)

var _ WorkflowResetter = (*workflowResetterImpl)(nil)

func NewWorkflowResetter(
	shardContext shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) *workflowResetterImpl {
	return &workflowResetterImpl{
		shardContext:      shardContext,
		namespaceRegistry: shardContext.GetNamespaceRegistry(),
		clusterMetadata:   shardContext.GetClusterMetadata(),
		executionMgr:      shardContext.GetExecutionManager(),
		workflowCache:     workflowCache,
		newStateRebuilder: func() StateRebuilder {
			return NewStateRebuilder(shardContext, logger)
		},
		transaction: workflow.NewTransaction(shardContext),
		logger:      logger,
	}
}

func (r *workflowResetterImpl) ResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	baseNextEventID int64,
	resetRunID string,
	resetRequestID string,
	currentWorkflow Workflow,
	resetReason string,
	additionalReapplyEvents []*historypb.HistoryEvent,
	resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
) (retError error) {

	namespaceEntry, err := r.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return err
	}
	resetWorkflowVersion := namespaceEntry.FailoverVersion()

	var currentWorkflowMutation *persistence.WorkflowMutation
	var currentWorkflowEventsSeq []*persistence.WorkflowEvents
	var reapplyEventsFn workflowResetReapplyEventsFn
	currentMutableState := currentWorkflow.GetMutableState()
	if currentMutableState.IsWorkflowExecutionRunning() {
		if err := r.terminateWorkflow(
			currentMutableState,
			resetReason,
		); err != nil {
			return err
		}
		resetWorkflowVersion = currentMutableState.GetCurrentVersion()

		currentWorkflowMutation, currentWorkflowEventsSeq, err = currentMutableState.CloseTransactionAsMutation(
			workflow.TransactionPolicyActive,
		)
		if err != nil {
			return err
		}

		reapplyEventsFn = func(ctx context.Context, resetMutableState workflow.MutableState) error {
			lastVisitedRunID, err := r.reapplyContinueAsNewWorkflowEvents(
				ctx,
				resetMutableState,
				namespaceID,
				workflowID,
				baseRunID,
				baseBranchToken,
				baseRebuildLastEventID+1,
				baseNextEventID,
				resetReapplyExcludeTypes,
			)
			if err != nil {
				return err
			}

			if lastVisitedRunID == currentMutableState.GetExecutionState().RunId {
				for _, event := range currentWorkflowEventsSeq {
					if err := reapplyEvents(resetMutableState, event.Events, resetReapplyExcludeTypes); err != nil {
						return err
					}
				}
			}
			return nil
		}
	} else {
		reapplyEventsFn = func(ctx context.Context, resetMutableState workflow.MutableState) error {
			_, err := r.reapplyContinueAsNewWorkflowEvents(
				ctx,
				resetMutableState,
				namespaceID,
				workflowID,
				baseRunID,
				baseBranchToken,
				baseRebuildLastEventID+1,
				baseNextEventID,
				resetReapplyExcludeTypes,
			)
			return err
		}
	}

	resetWorkflow, err := r.prepareResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetRequestID,
		resetWorkflowVersion,
		resetReason,
	)
	if err != nil {
		return err
	}
	defer func() { resetWorkflow.GetReleaseFn()(retError) }()

	resetMS := resetWorkflow.GetMutableState()
	if err := reapplyEventsFn(ctx, resetMS); err != nil {
		return err
	}
	if err := reapplyEvents(resetMS, additionalReapplyEvents, nil); err != nil {
		return err
	}

	if err := workflow.ScheduleWorkflowTask(resetWorkflow.GetMutableState()); err != nil {
		return err
	}

	return r.persistToDB(
		ctx,
		currentWorkflow,
		currentWorkflowMutation,
		currentWorkflowEventsSeq,
		resetWorkflow,
	)
}

func (r *workflowResetterImpl) prepareResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetRequestID string,
	resetWorkflowVersion int64,
	resetReason string,
) (Workflow, error) {

	resetWorkflow, err := r.replayResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetRequestID,
	)
	if err != nil {
		return nil, err
	}

	// Reset expiration time
	resetMutableState := resetWorkflow.GetMutableState()
	executionInfo := resetMutableState.GetExecutionInfo()

	weTimeout := timestamp.DurationValue(executionInfo.WorkflowExecutionTimeout)
	if weTimeout > 0 {
		executionInfo.WorkflowExecutionExpirationTime = timestamp.TimeNowPtrUtcAddDuration(weTimeout)
	}

	if resetMutableState.GetCurrentVersion() > resetWorkflowVersion {
		return nil, serviceerror.NewInternal("WorkflowResetter encountered version mismatch.")
	}
	if err := resetMutableState.UpdateCurrentVersion(
		resetWorkflowVersion,
		false,
	); err != nil {
		return nil, err
	}

	if len(resetMutableState.GetPendingChildExecutionInfos()) > 0 {
		return nil, serviceerror.NewInvalidArgument("WorkflowResetter encountered pending child workflows.")
	}

	if err := r.failWorkflowTask(
		resetMutableState,
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		resetRunID,
		resetReason,
	); err != nil {
		return nil, err
	}

	if err := r.failInflightActivity(
		resetMutableState.GetExecutionInfo().StartTime.AsTime(),
		resetMutableState,
		resetReason,
	); err != nil {
		return nil, err
	}

	return resetWorkflow, nil
}

func (r *workflowResetterImpl) persistToDB(
	ctx context.Context,
	currentWorkflow Workflow,
	currentWorkflowMutation *persistence.WorkflowMutation,
	currentWorkflowEventsSeq []*persistence.WorkflowEvents,
	resetWorkflow Workflow,
) error {

	resetWorkflowSnapshot, resetWorkflowEventsSeq, err := resetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return err
	}

	if currentWorkflowMutation != nil {
		if _, _, err := r.transaction.UpdateWorkflowExecution(
			ctx,
			persistence.UpdateWorkflowModeUpdateCurrent,
			currentWorkflow.GetMutableState().GetCurrentVersion(),
			currentWorkflowMutation,
			currentWorkflowEventsSeq,
			workflow.MutableStateFailoverVersion(resetWorkflow.GetMutableState()),
			resetWorkflowSnapshot,
			resetWorkflowEventsSeq,
		); err != nil {
			return err
		}
		return nil
	}

	currentMutableState := currentWorkflow.GetMutableState()
	currentRunID := currentMutableState.GetExecutionState().GetRunId()
	currentLastWriteVersion, err := currentMutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}

	return resetWorkflow.GetContext().CreateWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.CreateWorkflowModeUpdateCurrent,
		currentRunID,
		currentLastWriteVersion,
		resetWorkflow.GetMutableState(),
		resetWorkflowSnapshot,
		resetWorkflowEventsSeq,
	)
}

func (r *workflowResetterImpl) replayResetWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetRequestID string,
) (Workflow, error) {

	resetBranchToken, err := r.forkAndGenerateBranchToken(
		ctx,
		namespaceID,
		workflowID,
		baseBranchToken,
		baseRebuildLastEventID+1,
		resetRunID,
	)
	if err != nil {
		return nil, err
	}

	resetContext := workflow.NewContext(
		r.shardContext.GetConfig(),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			resetRunID,
		),
		r.logger,
		r.shardContext.GetLogger(),
		r.shardContext.GetMetricsHandler(),
	)
	resetMutableState, resetHistorySize, err := r.newStateRebuilder().Rebuild(
		ctx,
		r.shardContext.GetTimeSource().Now(),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			baseRunID,
		),
		baseBranchToken,
		baseRebuildLastEventID,
		util.Ptr(baseRebuildLastEventVersion),
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			resetRunID,
		),
		resetBranchToken,
		resetRequestID,
	)
	if err != nil {
		return nil, err
	}

	resetMutableState.SetBaseWorkflow(
		baseRunID,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
	)
	resetMutableState.AddHistorySize(resetHistorySize)
	return NewWorkflow(
		r.clusterMetadata,
		resetContext,
		resetMutableState,
		wcache.NoopReleaseFn,
	), nil
}

func (r *workflowResetterImpl) failWorkflowTask(
	resetMutableState workflow.MutableState,
	baseRunID string,
	baseRebuildLastEventID int64,
	baseRebuildLastEventVersion int64,
	resetRunID string,
	resetReason string,
) error {

	workflowTask := resetMutableState.GetPendingWorkflowTask()
	if workflowTask == nil {
		// TODO if resetMutableState.HadOrHasWorkflowTask() == true
		//  meaning workflow history has NO workflow task ever
		//  should also allow workflow reset, the only remaining issues are
		//  * what if workflow is a cron workflow, e.g. should add a workflow task directly or still respect the cron job
		return serviceerror.NewInvalidArgument(fmt.Sprintf(
			"Can only reset workflow to event ID in range [WorkflowTaskScheduled +1, WorkflowTaskStarted + 1]: %v",
			baseRebuildLastEventID+1,
		))
	}

	var err error
	if workflowTask.StartedEventID == common.EmptyVersion {
		_, workflowTask, err = resetMutableState.AddWorkflowTaskStartedEvent(
			workflowTask.ScheduledEventID,
			workflowTask.RequestID,
			workflowTask.TaskQueue,
			consts.IdentityHistoryService,
		)
		if err != nil {
			return err
		}
	}

	_, err = resetMutableState.AddWorkflowTaskFailedEvent(
		workflowTask,
		enumspb.WORKFLOW_TASK_FAILED_CAUSE_RESET_WORKFLOW,
		failure.NewResetWorkflowFailure(resetReason, nil),
		consts.IdentityHistoryService,
		"",
		baseRunID,
		resetRunID,
		baseRebuildLastEventVersion,
	)
	return err
}

func (r *workflowResetterImpl) failInflightActivity(
	now time.Time,
	mutableState workflow.MutableState,
	terminateReason string,
) error {

	for _, ai := range mutableState.GetPendingActivityInfos() {
		switch ai.StartedEventId {
		case common.EmptyEventID:
			// activity not started, noop
			// override the activity time to now
			ai.ScheduledTime = timestamppb.New(now)
			if err := mutableState.UpdateActivity(ai); err != nil {
				return err
			}

		case common.TransientEventID:
			// activity is started (with retry policy)
			// should not encounter this case when rebuilding mutable state
			return serviceerror.NewInternal("WorkflowResetter encountered transient activity.")

		default:
			if _, err := mutableState.AddActivityTaskFailedEvent(
				ai.ScheduledEventId,
				ai.StartedEventId,
				failure.NewResetWorkflowFailure(terminateReason, ai.LastHeartbeatDetails),
				enumspb.RETRY_STATE_NON_RETRYABLE_FAILURE,
				ai.StartedIdentity,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *workflowResetterImpl) forkAndGenerateBranchToken(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	forkBranchToken []byte,
	forkNodeID int64,
	resetRunID string,
) ([]byte, error) {
	// fork a new history branch
	shardID := r.shardContext.GetShardID()
	resp, err := r.executionMgr.ForkHistoryBranch(ctx, &persistence.ForkHistoryBranchRequest{
		ForkBranchToken: forkBranchToken,
		ForkNodeID:      forkNodeID,
		Info:            persistence.BuildHistoryGarbageCleanupInfo(namespaceID.String(), workflowID, resetRunID),
		ShardID:         shardID,
		NamespaceID:     namespaceID.String(),
	})
	if err != nil {
		return nil, err
	}

	return resp.NewBranchToken, nil
}

func (r *workflowResetterImpl) terminateWorkflow(
	mutableState workflow.MutableState,
	terminateReason string,
) error {

	return workflow.TerminateWorkflow(
		mutableState,
		terminateReason,
		nil,
		consts.IdentityResetter,
		false,
	)
}

func (r *workflowResetterImpl) reapplyContinueAsNewWorkflowEvents(
	ctx context.Context,
	resetMutableState workflow.MutableState,
	namespaceID namespace.ID,
	workflowID string,
	baseRunID string,
	baseBranchToken []byte,
	baseRebuildNextEventID int64,
	baseNextEventID int64,
	resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
) (string, error) {

	// TODO change this logic to fetching all workflow [baseWorkflow, currentWorkflow]
	//  from visibility for better coverage of events eligible for re-application.

	lastVisitedRunID := baseRunID

	// First, special handling of remaining events for base workflow
	nextRunID, err := r.reapplyWorkflowEvents(
		ctx,
		resetMutableState,
		baseRebuildNextEventID,
		baseNextEventID,
		baseBranchToken,
		resetReapplyExcludeTypes,
	)
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		r.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(workflowID), tag.WorkflowRunID(baseRunID))
		return "", err
	default:
		return "", err
	}

	getNextEventIDBranchToken := func(runID string) (nextEventID int64, branchToken []byte, retError error) {
		context, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
			ctx,
			r.shardContext,
			namespaceID,
			&commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return 0, nil, err
		}
		defer func() { release(retError) }()

		mutableState, err := context.LoadMutableState(ctx, r.shardContext)
		if err != nil {
			// no matter what error happen, we need to retry
			return 0, nil, err
		}

		nextEventID = mutableState.GetNextEventID()
		branchToken, err = mutableState.GetCurrentBranchToken()
		if err != nil {
			return 0, nil, err
		}
		return nextEventID, branchToken, nil
	}

	// Second, for remaining continue as new workflow, reapply eligible events
	for len(nextRunID) != 0 {
		lastVisitedRunID = nextRunID
		nextWorkflowNextEventID, nextWorkflowBranchToken, err := getNextEventIDBranchToken(nextRunID)
		if err != nil {
			return "", err
		}

		nextRunID, err = r.reapplyWorkflowEvents(
			ctx,
			resetMutableState,
			common.FirstEventID,
			nextWorkflowNextEventID,
			nextWorkflowBranchToken,
			resetReapplyExcludeTypes,
		)
		switch err.(type) {
		case nil:
			// noop
		case *serviceerror.DataLoss:
			// log event
			r.logger.Error("encounter data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(workflowID), tag.WorkflowRunID(nextRunID))
			return "", err
		default:
			return "", err
		}
	}
	return lastVisitedRunID, nil
}

func (r *workflowResetterImpl) reapplyWorkflowEvents(
	ctx context.Context,
	mutableState workflow.MutableState,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
	resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
) (string, error) {

	// TODO change this logic to fetching all workflow [baseWorkflow, currentWorkflow]
	//  from visibility for better coverage of events eligible for re-application.
	//  after the above change, this API do not have to return the continue as new run ID

	iter := collection.NewPagingIterator(r.getPaginationFn(
		ctx,
		firstEventID,
		nextEventID,
		branchToken,
	))

	var nextRunID string
	var lastEvents []*historypb.HistoryEvent

	for iter.HasNext() {
		batch, err := iter.Next()
		if err != nil {
			return "", err
		}
		lastEvents = batch.Events
		if err := reapplyEvents(mutableState, lastEvents, resetReapplyExcludeTypes); err != nil {
			return "", err
		}
	}

	if len(lastEvents) > 0 {
		lastEvent := lastEvents[len(lastEvents)-1]
		if lastEvent.GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW {
			nextRunID = lastEvent.GetWorkflowExecutionContinuedAsNewEventAttributes().GetNewExecutionRunId()
		}
	}
	return nextRunID, nil
}

func reapplyEvents(
	mutableState workflow.MutableState,
	events []*historypb.HistoryEvent,
	resetReapplyExcludeTypes []enumspb.ResetReapplyExcludeType,
) error {
	excludeSignal := false
	for _, e := range resetReapplyExcludeTypes {
		if e == enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL {
			excludeSignal = true
		}
	}

	for _, event := range events {
		switch event.GetEventType() {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:
			if excludeSignal {
				continue
			}
			attr := event.GetWorkflowExecutionSignaledEventAttributes()
			if _, err := mutableState.AddWorkflowExecutionSignaled(
				attr.GetSignalName(),
				attr.GetInput(),
				attr.GetIdentity(),
				attr.GetHeader(),
				attr.GetSkipGenerateWorkflowTask(),
			); err != nil {
				return err
			}
		default:
			// events other than signal will be ignored
		}
	}
	return nil
}

func (r *workflowResetterImpl) getPaginationFn(
	ctx context.Context,
	firstEventID int64,
	nextEventID int64,
	branchToken []byte,
) collection.PaginationFn[*historypb.History] {

	return func(paginationToken []byte) ([]*historypb.History, []byte, error) {

		resp, err := r.executionMgr.ReadHistoryBranchByBatch(ctx, &persistence.ReadHistoryBranchRequest{
			BranchToken:   branchToken,
			MinEventID:    firstEventID,
			MaxEventID:    nextEventID,
			PageSize:      defaultPageSize,
			NextPageToken: paginationToken,
			ShardID:       r.shardContext.GetShardID(),
		})
		if err != nil {
			return nil, nil, err
		}
		return resp.History, resp.NextPageToken, nil
	}
}

func IsTerminatedByResetter(event *historypb.HistoryEvent) bool {
	if attributes := event.GetWorkflowExecutionTerminatedEventAttributes(); attributes != nil && attributes.Identity == consts.IdentityResetter {
		return true
	}
	return false
}
