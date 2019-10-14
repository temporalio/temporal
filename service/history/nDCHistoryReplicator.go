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

package history

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
)

type (
	nDCBranchMgrProvider func(
		context workflowExecutionContext,
		mutableState mutableState,
		logger log.Logger,
	) nDCBranchMgr

	nDCConflictResolverProvider func(
		context workflowExecutionContext,
		mutableState mutableState,
		logger log.Logger,
	) nDCConflictResolver

	nDCWorkflowResetterProvider func(
		domainID string,
		workflowID string,
		baseRunID string,
		newContext workflowExecutionContext,
		newRunID string,
		logger log.Logger,
	) nDCWorkflowResetter

	nDCHistoryReplicator interface {
		ApplyEvents(
			ctx ctx.Context,
			request *h.ReplicateEventsV2Request,
		) error
	}

	nDCHistoryReplicatorImpl struct {
		shard             ShardContext
		clusterMetadata   cluster.Metadata
		historyV2Mgr      persistence.HistoryV2Manager
		historySerializer persistence.PayloadSerializer
		metricsClient     metrics.Client
		domainCache       cache.DomainCache
		historyCache      *historyCache
		eventsReapplier   nDCEventsReapplier
		transactionMgr    nDCTransactionMgr
		logger            log.Logger

		newBranchMgr        nDCBranchMgrProvider
		newConflictResolver nDCConflictResolverProvider
		newWorkflowResetter nDCWorkflowResetterProvider
		newStateBuilder     stateBuilderProvider
		newMutableState     mutableStateProvider
	}
)

var errPanic = errors.NewInternalFailureError("encounter panic")

func newNDCHistoryReplicator(
	shard ShardContext,
	historyCache *historyCache,
	eventsReapplier nDCEventsReapplier,
	logger log.Logger,
) *nDCHistoryReplicatorImpl {

	transactionMgr := newNDCTransactionMgr(shard, historyCache, eventsReapplier, logger)
	replicator := &nDCHistoryReplicatorImpl{
		shard:             shard,
		clusterMetadata:   shard.GetService().GetClusterMetadata(),
		historyV2Mgr:      shard.GetHistoryV2Manager(),
		historySerializer: persistence.NewPayloadSerializer(),
		metricsClient:     shard.GetMetricsClient(),
		domainCache:       shard.GetDomainCache(),
		historyCache:      historyCache,
		transactionMgr:    transactionMgr,
		eventsReapplier:   eventsReapplier,
		logger:            logger.WithTags(tag.ComponentHistoryReplicator),

		newBranchMgr: func(
			context workflowExecutionContext,
			mutableState mutableState,
			logger log.Logger,
		) nDCBranchMgr {
			return newNDCBranchMgr(shard, context, mutableState, logger)
		},
		newConflictResolver: func(
			context workflowExecutionContext,
			mutableState mutableState,
			logger log.Logger,
		) nDCConflictResolver {
			return newNDCConflictResolver(shard, context, mutableState, logger)
		},
		newWorkflowResetter: func(
			domainID string,
			workflowID string,
			baseRunID string,
			newContext workflowExecutionContext,
			newRunID string,
			logger log.Logger,
		) nDCWorkflowResetter {
			return newNDCWorkflowResetter(shard, transactionMgr, domainID, workflowID, baseRunID, newContext, newRunID, logger)
		},
		newStateBuilder: func(
			msBuilder mutableState,
			logger log.Logger,
		) stateBuilder {
			return newStateBuilder(shard, msBuilder, logger)
		},
		newMutableState: func(
			domainEntry *cache.DomainCacheEntry,
			logger log.Logger,
		) mutableState {
			return newMutableStateBuilderWithVersionHistories(
				shard,
				shard.GetEventsCache(),
				logger,
				domainEntry,
			)
		},
	}

	return replicator
}

func (r *nDCHistoryReplicatorImpl) ApplyEvents(
	ctx ctx.Context,
	request *h.ReplicateEventsV2Request,
) (retError error) {

	startTime := time.Now()
	task, err := newNDCReplicationTask(
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

func (r *nDCHistoryReplicatorImpl) applyEvents(
	ctx ctx.Context,
	task nDCReplicationTask,
) (retError error) {

	context, releaseFn, err := r.historyCache.getOrCreateWorkflowExecution(
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
		mutableState, err := context.loadWorkflowExecution()
		switch err.(type) {
		case nil:
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

func (r *nDCHistoryReplicatorImpl) applyStartEvents(
	ctx ctx.Context,
	context workflowExecutionContext,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) (retError error) {

	domainEntry, err := r.domainCache.GetDomainByID(task.getDomainID())
	if err != nil {
		return err
	}
	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	mutableState := r.newMutableState(domainEntry, task.getLogger())
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())

	// use state builder for workflow mutable state mutation
	_, _, _, err = stateBuilder.applyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		return err
	}

	err = r.transactionMgr.createWorkflow(
		ctx,
		task.getEventTime(),
		newNDCWorkflow(
			ctx,
			r.domainCache,
			r.clusterMetadata,
			context,
			mutableState,
			releaseFn,
		),
	)
	if err == nil {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsPrepareBranch(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) (bool, int, error) {

	incomingVersionHistory := task.getVersionHistory()
	branchMgr := r.newBranchMgr(context, mutableState, task.getLogger())
	doContinue, versionHistoryIndex, err := branchMgr.prepareVersionHistory(
		ctx,
		incomingVersionHistory,
		task.getFirstEvent().GetEventId(),
	)
	if err != nil {
		return false, 0, err
	}
	return doContinue, versionHistoryIndex, nil

}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsPrepareMutableState(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int,
	task nDCReplicationTask,
) (mutableState, bool, error) {

	incomingVersion := task.getVersion()
	conflictResolver := r.newConflictResolver(context, mutableState, task.getLogger())
	return conflictResolver.prepareMutableState(
		ctx,
		branchIndex,
		incomingVersion,
	)
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToCurrentBranch(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	isRebuilt bool,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	_, _, newMutableState, err := stateBuilder.applyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		return err
	}

	targetWorkflow := newNDCWorkflow(
		ctx,
		r.domainCache,
		r.clusterMetadata,
		context,
		mutableState,
		releaseFn,
	)

	var newWorkflow nDCWorkflow
	if newMutableState != nil {
		newExecutionInfo := newMutableState.GetExecutionInfo()
		newContext := newWorkflowExecutionContext(
			newExecutionInfo.DomainID,
			shared.WorkflowExecution{
				WorkflowId: common.StringPtr(newExecutionInfo.WorkflowID),
				RunId:      common.StringPtr(newExecutionInfo.RunID),
			},
			r.shard,
			r.shard.GetExecutionManager(),
			r.logger,
		)

		newWorkflow = newNDCWorkflow(
			ctx,
			r.domainCache,
			r.clusterMetadata,
			newContext,
			newMutableState,
			noopReleaseFn,
		)
	}

	err = r.transactionMgr.updateWorkflow(
		ctx,
		task.getEventTime(),
		isRebuilt,
		targetWorkflow,
		newWorkflow,
	)
	if err == nil {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsToNoneCurrentBranch(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	branchIndex int,
	releaseFn releaseWorkflowExecutionFunc,
	task nDCReplicationTask,
) error {

	// workflow backfill to non current branch
	// if encounter backfill with continue as new
	// first, create the new workflow as zombie
	if len(task.getNewEvents()) != 0 {
		startTime := time.Now()
		newTask, err := task.generateNewRunTask(startTime)
		if err != nil {
			return err
		}
		if err := r.applyEvents(ctx, newTask); err != nil {
			return err
		}
	}

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

	return r.transactionMgr.backfillWorkflow(
		ctx,
		task.getEventTime(),
		newNDCWorkflow(
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
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsMissingMutableState(
	ctx ctx.Context,
	newContext workflowExecutionContext,
	task nDCReplicationTask,
) (mutableState, error) {

	// for non reset workflow execution replication task, just do re-application
	if !task.isWorkflowReset() {
		return nil, newNDCRetryTaskErrorWithHint()
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

	return workflowResetter.resetWorkflow(ctx, task.getEventTime(), baseEventID, baseEventVersion)
}

func (r *nDCHistoryReplicatorImpl) applyNonStartEventsResetWorkflow(
	ctx ctx.Context,
	context workflowExecutionContext,
	mutableState mutableState,
	task nDCReplicationTask,
) error {

	requestID := uuid.New() // requestID used for start workflow execution request.  This is not on the history event.
	stateBuilder := r.newStateBuilder(mutableState, task.getLogger())
	_, _, _, err := stateBuilder.applyEvents(
		task.getDomainID(),
		requestID,
		*task.getExecution(),
		task.getEvents(),
		task.getNewEvents(),
		true,
	)
	if err != nil {
		return err
	}

	targetWorkflow := newNDCWorkflow(
		ctx,
		r.domainCache,
		r.clusterMetadata,
		context,
		mutableState,
		noopReleaseFn,
	)

	err = r.transactionMgr.createWorkflow(
		ctx,
		task.getEventTime(),
		targetWorkflow,
	)
	if err == nil {
		r.notify(task.getSourceCluster(), task.getEventTime())
	}
	return err
}

func (r *nDCHistoryReplicatorImpl) notify(
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

func newNDCRetryTaskErrorWithHint() error {
	// TODO add detail info here
	return &shared.RetryTaskV2Error{}
}
