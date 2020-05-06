// Copyright (c) 2019 Uber Technologies, Inc.
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

package ndc

import (
	ctx "context"
	"time"

	"github.com/pborman/uuid"

	h "github.com/uber/cadence/.gen/go/history"
	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/errors"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/shard"
)

const (
	mutableStateMissingMessage = "Resend events due to missing mutable state"
)

type (
	// HistoryReplicator handles history replication task
	HistoryReplicator interface {
		ApplyEvents(
			ctx ctx.Context,
			request *h.ReplicateEventsV2Request,
		) error
	}

	historyReplicatorImpl struct {
		shard              shard.Context
		clusterMetadata    cluster.Metadata
		historyV2Manager   persistence.HistoryManager
		historySerializer  persistence.PayloadSerializer
		metricsClient      metrics.Client
		domainCache        cache.DomainCache
		executionCache     *execution.Cache
		eventsReapplier    EventsReapplier
		transactionManager transactionManager
		logger             log.Logger

		newBranchManager    branchManagerProvider
		newConflictResolver conflictResolverProvider
		newWorkflowResetter workflowResetterProvider
		newStateBuilder     stateBuilderProvider
		newMutableState     mutableStateProvider
	}

	stateBuilderProvider func(
		mutableState execution.MutableState,
		logger log.Logger) execution.StateBuilder

	mutableStateProvider func(
		domainEntry *cache.DomainCacheEntry,
		logger log.Logger,
	) execution.MutableState

	branchManagerProvider func(
		context execution.Context,
		mutableState execution.MutableState,
		logger log.Logger,
	) branchManager

	conflictResolverProvider func(
		context execution.Context,
		mutableState execution.MutableState,
		logger log.Logger,
	) conflictResolver

	workflowResetterProvider func(
		domainID string,
		workflowID string,
		baseRunID string,
		newContext execution.Context,
		newRunID string,
		logger log.Logger,
	) WorkflowResetter
)

var _ HistoryReplicator = (*historyReplicatorImpl)(nil)

var errPanic = errors.NewInternalFailureError("encounter panic")

// NewHistoryReplicator creates history replicator
func NewHistoryReplicator(
	shard shard.Context,
	executionCache *execution.Cache,
	eventsReapplier EventsReapplier,
	logger log.Logger,
) HistoryReplicator {

	transactionManager := newTransactionManager(shard, executionCache, eventsReapplier, logger)
	replicator := &historyReplicatorImpl{
		shard:              shard,
		clusterMetadata:    shard.GetService().GetClusterMetadata(),
		historyV2Manager:   shard.GetHistoryManager(),
		historySerializer:  persistence.NewPayloadSerializer(),
		metricsClient:      shard.GetMetricsClient(),
		domainCache:        shard.GetDomainCache(),
		executionCache:     executionCache,
		transactionManager: transactionManager,
		eventsReapplier:    eventsReapplier,
		logger:             logger.WithTags(tag.ComponentHistoryReplicator),

		newBranchManager: func(
			context execution.Context,
			mutableState execution.MutableState,
			logger log.Logger,
		) branchManager {
			return newBranchManager(shard, context, mutableState, logger)
		},
		newConflictResolver: func(
			context execution.Context,
			mutableState execution.MutableState,
			logger log.Logger,
		) conflictResolver {
			return newConflictResolver(shard, context, mutableState, logger)
		},
		newWorkflowResetter: func(
			domainID string,
			workflowID string,
			baseRunID string,
			newContext execution.Context,
			newRunID string,
			logger log.Logger,
		) WorkflowResetter {
			return NewWorkflowResetter(shard, transactionManager, domainID, workflowID, baseRunID, newContext, newRunID, logger)
		},
		newStateBuilder: func(
			state execution.MutableState,
			logger log.Logger,
		) execution.StateBuilder {

			return execution.NewStateBuilder(
				shard,
				logger,
				state,
				func(mutableState execution.MutableState) execution.MutableStateTaskGenerator {
					return execution.NewMutableStateTaskGenerator(shard.GetDomainCache(), logger, mutableState)
				},
			)
		},
		newMutableState: func(
			domainEntry *cache.DomainCacheEntry,
			logger log.Logger,
		) execution.MutableState {
			return execution.NewMutableStateBuilderWithVersionHistories(
				shard,
				logger,
				domainEntry,
			)
		},
	}

	return replicator
}

func (r *historyReplicatorImpl) ApplyEvents(
	ctx ctx.Context,
	request *h.ReplicateEventsV2Request,
) (retError error) {

	startTime := time.Now()
	task, err := newReplicationTask(
		r.clusterMetadata,
		r.historySerializer,
		startTime,
		r.logger,
		request,
	)
	if err != nil {
		return err
	}

	return r.applyEvents(ctx, task)
}

func (r *historyReplicatorImpl) applyEvents(
	ctx ctx.Context,
	task replicationTask,
) (retError error) {

	context, releaseFn, err := r.executionCache.GetOrCreateWorkflowExecution(
		ctx,
		task.getDomainID(),
		*task.getExecution(),
	)
	if err != nil {
		// for get workflow execution context, with valid run id
		// err will not be of type EntityNotExistsError
		return err
	}
	defer func() {
		if rec := recover(); rec != nil {
			releaseFn(errPanic)
			panic(rec)
		} else {
			releaseFn(retError)
		}
	}()

	switch task.getFirstEvent().GetEventType() {
	case shared.EventTypeWorkflowExecutionStarted:
		return r.applyStartEvents(ctx, context, releaseFn, task)

	default:
		// apply events, other than simple start workflow execution
		// the continue as new + start workflow execution combination will also be processed here
		var mutableState execution.MutableState
		var err error
		domainEntry, err := r.domainCache.GetDomainByID(context.GetDomainID())
		if err != nil {
			return err
		}

		if r.shard.GetConfig().ReplicationEventsFromCurrentCluster(domainEntry.GetInfo().Name) {
			// this branch is used when replicating events (generated from current cluster)from remote cluster to current cluster.
			// this could happen when the events are lost in current cluster and plan to recover them from remote cluster.
			mutableState, err = context.LoadWorkflowExecutionForReplication(task.getVersion())
		} else {
			mutableState, err = context.LoadWorkflowExecution()
		}
		switch err.(type) {
		case nil:
			// Sanity check to make only 3DC mutable state here
			if mutableState.GetVersionHistories() == nil {
				return &shared.InternalServiceError{Message: "The mutable state does not support 3DC."}
			}

			doContinue, branchIndex, err := r.applyNonStartEventsPrepareBranch(ctx, context, mutableState, task)
			if err != nil {
				return err
			} else if !doContinue {
				r.metricsClient.IncCounter(metrics.ReplicateHistoryEventsScope, metrics.DuplicateReplicationEventsCounter)
				return nil
			}

			mutableState, isRebuilt, err := r.applyNonStartEventsPrepareMutableState(ctx, context, mutableState, branchIndex, task)
			if err != nil {
				return err
			}

			if mutableState.GetVersionHistories().GetCurrentVersionHistoryIndex() == branchIndex {
				return r.applyNonStartEventsToCurrentBranch(ctx, context, mutableState, isRebuilt, releaseFn, task)
			}
			return r.applyNonStartEventsToNoneCurrentBranch(ctx, context, mutableState, branchIndex, releaseFn, task)

		case *shared.EntityNotExistsError:
			// mutable state not created, check if is workflow reset
			mutableState, err := r.applyNonStartEventsMissingMutableState(ctx, context, task)
			if err != nil {
				return err
			}

			return r.applyNonStartEventsResetWorkflow(ctx, context, mutableState, task)

		default:
			// unable to get mutable state, return err so we can retry the task later
			return err
		}
	}
}

func (r *historyReplicatorImpl) applyStartEvents(
	ctx ctx.Context,
	context execution.Context,
	releaseFn execution.ReleaseFunc,
	task replicationTask,
) (retError error) {

	domainEntry, err := r.domainCache.GetDomainByID(task.getDomainID())
	if err != nil {
		return err
	}
	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	mutableState := r.newMutableState(domainEntry, task.getLogger())
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())

	// use state builder for workflow mutable state mutation
	_, err = stateBuilder.ApplyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyStartEvents",
			tag.Error(err),
		)
		return err
	}

	err = r.transactionManager.createWorkflow(
		ctx,
		task.getEventTime(),
		execution.NewWorkflow(
			ctx,
			r.domainCache,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyStartEvents",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *historyReplicatorImpl) applyNonStartEventsPrepareBranch(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	task replicationTask,
) (bool, int, error) {

	incomingVersionHistory := task.getVersionHistory()
	branchManager := r.newBranchManager(context, mutableState, task.getLogger())
	doContinue, versionHistoryIndex, err := branchManager.prepareVersionHistory(
		ctx,
		incomingVersionHistory,
		task.getFirstEvent().GetEventId(),
		task.getFirstEvent().GetVersion(),
	)
	switch err.(type) {
	case nil:
		return doContinue, versionHistoryIndex, nil
	case *shared.RetryTaskV2Error:
		// replication message can arrive out of order
		// do not log
		return false, 0, err
	default:
		task.getLogger().Error(
			"nDCHistoryReplicator unable to prepare version history when applyNonStartEventsPrepareBranch",
			tag.Error(err),
		)
		return false, 0, err
	}
}

func (r *historyReplicatorImpl) applyNonStartEventsPrepareMutableState(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	branchIndex int,
	task replicationTask,
) (execution.MutableState, bool, error) {

	incomingVersion := task.getVersion()
	conflictResolver := r.newConflictResolver(context, mutableState, task.getLogger())
	mutableState, isRebuilt, err := conflictResolver.prepareMutableState(
		ctx,
		branchIndex,
		incomingVersion,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to prepare mutable state when applyNonStartEventsPrepareMutableState",
			tag.Error(err),
		)
	}
	return mutableState, isRebuilt, err
}

func (r *historyReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	isRebuilt bool,
	releaseFn execution.ReleaseFunc,
	task replicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	newMutableState, err := stateBuilder.ApplyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyNonStartEventsToCurrentBranch",
			tag.Error(err),
		)
		return err
	}

	targetWorkflow := execution.NewWorkflow(
		ctx,
		r.domainCache,
		r.clusterMetadata,
		context,
		mutableState,
		releaseFn,
	)

	var newWorkflow execution.Workflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newContext := execution.NewContext(
			newExecutionInfo.DomainID,
			shared.WorkflowExecution{
				WorkflowId: common.StringPtr(newExecutionInfo.WorkflowID),
				RunId:      common.StringPtr(newExecutionInfo.RunID),
			},
			r.shard,
			r.shard.GetExecutionManager(),
			r.logger,
		)

		newWorkflow = execution.NewWorkflow(
			ctx,
			r.domainCache,
			r.clusterMetadata,
			newContext,
			newMutableState,
			execution.NoopReleaseFn,
		)
	}

	err = r.transactionManager.updateWorkflow(
		ctx,
		task.getEventTime(),
		isRebuilt,
		targetWorkflow,
		newWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to update workflow when applyNonStartEventsToCurrentBranch",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *historyReplicatorImpl) applyNonStartEventsToNoneCurrentBranch(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	branchIndex int,
	releaseFn execution.ReleaseFunc,
	task replicationTask,
) error {

	if len(task.getNewEvents()) != 0 {
		return r.applyNonStartEventsToNoneCurrentBranchWithContinueAsNew(
			ctx,
			context,
			releaseFn,
			task,
		)
	}

	return r.applyNonStartEventsToNoneCurrentBranchWithoutContinueAsNew(
		ctx,
		context,
		mutableState,
		branchIndex,
		releaseFn,
		task,
	)
}

func (r *historyReplicatorImpl) applyNonStartEventsToNoneCurrentBranchWithoutContinueAsNew(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	branchIndex int,
	releaseFn execution.ReleaseFunc,
	task replicationTask,
) error {

	versionHistoryItem := persistence.NewVersionHistoryItem(
		task.getLastEvent().GetEventId(),
		task.getLastEvent().GetVersion(),
	)
	versionHistory, err := mutableState.GetVersionHistories().GetVersionHistory(branchIndex)
	if err != nil {
		return err
	}
	if err = versionHistory.AddOrUpdateItem(versionHistoryItem); err != nil {
		return err
	}

	err = r.transactionManager.backfillWorkflow(
		ctx,
		task.getEventTime(),
		execution.NewWorkflow(
			ctx,
			r.domainCache,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
		&persistence.WorkflowEvents{
			DomainID:    task.getDomainID(),
			WorkflowID:  task.getExecution().GetWorkflowId(),
			RunID:       task.getExecution().GetRunId(),
			BranchToken: versionHistory.GetBranchToken(),
			Events:      task.getEvents(),
		},
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to backfill workflow when applyNonStartEventsToNoneCurrentBranch",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *historyReplicatorImpl) applyNonStartEventsToNoneCurrentBranchWithContinueAsNew(
	ctx ctx.Context,
	context execution.Context,
	releaseFn execution.ReleaseFunc,
	task replicationTask,
) error {

	// workflow backfill to non current branch with continue as new
	// first, release target workflow lock & create the new workflow as zombie
	// NOTE: need to release target workflow due to target workflow
	//  can potentially be the current workflow causing deadlock

	// 1. clear all in memory changes & release target workflow lock
	// 2. apply new workflow first
	// 3. apply target workflow

	// step 1
	context.Clear()
	releaseFn(nil)

	// step 2
	startTime := time.Now()
	task, newTask, err := task.splitTask(startTime)
	if err != nil {
		return err
	}
	if err := r.applyEvents(ctx, newTask); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create new workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}

	// step 3
	if err := r.applyEvents(ctx, task); err != nil {
		newTask.getLogger().Error(
			"nDCHistoryReplicator unable to create target workflow when applyNonStartEventsToNoneCurrentBranchWithContinueAsNew",
			tag.Error(err),
		)
		return err
	}
	return nil
}

func (r *historyReplicatorImpl) applyNonStartEventsMissingMutableState(
	ctx ctx.Context,
	newContext execution.Context,
	task replicationTask,
) (execution.MutableState, error) {

	// for non reset workflow execution replication task, just do re-replication
	if !task.isWorkflowReset() {
		firstEvent := task.getFirstEvent()
		return nil, newNDCRetryTaskErrorWithHint(
			mutableStateMissingMessage,
			task.getDomainID(),
			task.getWorkflowID(),
			task.getRunID(),
			nil,
			nil,
			common.Int64Ptr(firstEvent.GetEventId()),
			common.Int64Ptr(firstEvent.GetVersion()),
		)
	}

	decisionTaskFailedEvent := task.getFirstEvent()
	attr := decisionTaskFailedEvent.DecisionTaskFailedEventAttributes
	baseRunID := attr.GetBaseRunId()
	baseEventID := decisionTaskFailedEvent.GetEventId() - 1
	baseEventVersion := attr.GetForkEventVersion()
	newRunID := attr.GetNewRunId()

	workflowResetter := r.newWorkflowResetter(
		task.getDomainID(),
		task.getWorkflowID(),
		baseRunID,
		newContext,
		newRunID,
		task.getLogger(),
	)

	resetMutableState, err := workflowResetter.ResetWorkflow(
		ctx,
		task.getEventTime(),
		baseEventID,
		baseEventVersion,
		task.getFirstEvent().GetEventId(),
		task.getVersion(),
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to reset workflow when applyNonStartEventsMissingMutableState",
			tag.Error(err),
		)
		return nil, err
	}
	return resetMutableState, nil
}

func (r *historyReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx ctx.Context,
	context execution.Context,
	mutableState execution.MutableState,
	task replicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	_, err := stateBuilder.ApplyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to apply events when applyNonStartEventsResetWorkflow",
			tag.Error(err),
		)
		return err
	}

	targetWorkflow := execution.NewWorkflow(
		ctx,
		r.domainCache,
		r.clusterMetadata,
		context,
		mutableState,
		execution.NoopReleaseFn,
	)

	err = r.transactionManager.createWorkflow(
		ctx,
		task.getEventTime(),
		targetWorkflow,
	)
	if err != nil {
		task.getLogger().Error(
			"nDCHistoryReplicator unable to create workflow when applyNonStartEventsResetWorkflow",
			tag.Error(err),
		)
	} else {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *historyReplicatorImpl) notify(
	clusterName string,
	now time.Time,
) {
	if clusterName == r.clusterMetadata.GetCurrentClusterName() {
		// this is a valid use case for testing, but not for production
		r.logger.Warn("nDCHistoryReplicator applying events generated by current cluster")
		return
	}
	now = now.Add(-r.shard.GetConfig().StandbyClusterDelay())
	r.shard.SetCurrentTime(clusterName, now)
}

func newNDCRetryTaskErrorWithHint(
	message string,
	domainID string,
	workflowID string,
	runID string,
	startEventID *int64,
	startEventVersion *int64,
	endEventID *int64,
	endEventVersion *int64,
) error {

	return &shared.RetryTaskV2Error{
		Message:           message,
		DomainId:          common.StringPtr(domainID),
		WorkflowId:        common.StringPtr(workflowID),
		RunId:             common.StringPtr(runID),
		StartEventId:      startEventID,
		StartEventVersion: startEventVersion,
		EndEventId:        endEventID,
		EndEventVersion:   endEventVersion,
	}
}
