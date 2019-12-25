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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCTransactionMgr_mock.go

package history

import (
	ctx "context"
	"time"

	"github.com/pborman/uuid"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
)

// NOTE: terminology
//
// 1. currentWorkflow means current running / closed workflow
//  pointed by the current record in DB
//
// 2. targetWorkflow means the workflow to be replicated
//  pointed by the replication task
//
// 3. newWorkflow means the workflow to be replicated, as part of continue as new
//  pointed by the replication task
//
// 4. if target workflow and current workflow are the same
//  then target workflow is set, current workflow is nil
//
// 5. suppress a workflow means turn a workflow into a zombie
//  or terminate a workflow

// Cases to be handled by this file:
//
// create path (there will be only current branch)
// 1. create as current															-> nDCTransactionPolicyCreateAsCurrent
// 2. create as zombie															-> nDCTransactionPolicyCreateAsZombie
//
// create path (there will be only current branch) + suppress current
// 1. create as current & suppress current										-> nDCTransactionPolicySuppressCurrentAndCreateAsCurrent
//
// update to current branch path
// 1. update as current															-> nDCTransactionPolicyUpdateAsCurrent
// 2. update as current & new created as current								-> nDCTransactionPolicyUpdateAsCurrent
// 3. update as zombie															-> nDCTransactionPolicyUpdateAsZombie
// 4. update as zombie & new created as zombie									-> nDCTransactionPolicyUpdateAsZombie
//
// backfill to non current branch path
// 1. backfill as is
// 2. backfill as is & new created as zombie
//
// conflict resolve path
// 1. conflict resolve as current												-> nDCTransactionPolicyConflictResolveAsCurrent
// 2. conflict resolve as zombie												-> nDCTransactionPolicyConflictResolveAsZombie
//
// conflict resolve path + suppress current
// 1. update from zombie to current & suppress current							-> nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent
// 2. update from zombie to current & new created as current & suppress current	-> nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent

type nDCTransactionPolicy int

const (
	nDCTransactionPolicyCreateAsCurrent nDCTransactionPolicy = iota
	nDCTransactionPolicyCreateAsZombie
	nDCTransactionPolicySuppressCurrentAndCreateAsCurrent

	nDCTransactionPolicyUpdateAsCurrent
	nDCTransactionPolicyUpdateAsZombie

	nDCTransactionPolicyConflictResolveAsCurrent
	nDCTransactionPolicyConflictResolveAsZombie

	nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent
)

const (
	eventsReapplicationResetWorkflowReason = "events-reapplication"
)

type (
	nDCTransactionMgr interface {
		createWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow nDCWorkflow,
		) error
		updateWorkflow(
			ctx ctx.Context,
			now time.Time,
			isWorkflowRebuilt bool,
			targetWorkflow nDCWorkflow,
			newWorkflow nDCWorkflow,
		) error
		backfillWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow nDCWorkflow,
			targetWorkflowEvents *persistence.WorkflowEvents,
		) error

		checkWorkflowExists(
			ctx ctx.Context,
			domainID string,
			workflowID string,
			runID string,
		) (bool, error)
		getCurrentWorkflowRunID(
			ctx ctx.Context,
			domainID string,
			workflowID string,
		) (string, error)
		loadNDCWorkflow(
			ctx ctx.Context,
			domainID string,
			workflowID string,
			runID string,
		) (nDCWorkflow, error)
	}

	nDCTransactionMgrImpl struct {
		shard            ShardContext
		domainCache      cache.DomainCache
		historyCache     *historyCache
		clusterMetadata  cluster.Metadata
		historyV2Mgr     persistence.HistoryManager
		serializer       persistence.PayloadSerializer
		metricsClient    metrics.Client
		workflowResetter workflowResetter
		eventsReapplier  nDCEventsReapplier
		logger           log.Logger

		createMgr nDCTransactionMgrForNewWorkflow
		updateMgr nDCTransactionMgrForExistingWorkflow
	}
)

var _ nDCTransactionMgr = (*nDCTransactionMgrImpl)(nil)

func newNDCTransactionMgr(
	shard ShardContext,
	historyCache *historyCache,
	eventsReapplier nDCEventsReapplier,
	logger log.Logger,
) *nDCTransactionMgrImpl {

	transactionMgr := &nDCTransactionMgrImpl{
		shard:           shard,
		domainCache:     shard.GetDomainCache(),
		historyCache:    historyCache,
		clusterMetadata: shard.GetClusterMetadata(),
		historyV2Mgr:    shard.GetHistoryManager(),
		serializer:      shard.GetService().GetPayloadSerializer(),
		metricsClient:   shard.GetMetricsClient(),
		workflowResetter: newWorkflowResetter(
			shard,
			historyCache,
			logger,
		),
		eventsReapplier: eventsReapplier,
		logger:          logger.WithTags(tag.ComponentHistoryReplicator),

		createMgr: nil,
		updateMgr: nil,
	}
	transactionMgr.createMgr = newNDCTransactionMgrForNewWorkflow(transactionMgr)
	transactionMgr.updateMgr = newNDCTransactionMgrForExistingWorkflow(transactionMgr)
	return transactionMgr
}

func (r *nDCTransactionMgrImpl) createWorkflow(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow nDCWorkflow,
) error {

	return r.createMgr.dispatchForNewWorkflow(
		ctx,
		now,
		targetWorkflow,
	)
}

func (r *nDCTransactionMgrImpl) updateWorkflow(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow nDCWorkflow,
	newWorkflow nDCWorkflow,
) error {

	return r.updateMgr.dispatchForExistingWorkflow(
		ctx,
		now,
		isWorkflowRebuilt,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *nDCTransactionMgrImpl) backfillWorkflow(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow nDCWorkflow,
	targetWorkflowEvents *persistence.WorkflowEvents,
) (retError error) {

	defer func() {
		if rec := recover(); rec != nil {
			targetWorkflow.getReleaseFn()(errPanic)
			panic(rec)
		} else {
			targetWorkflow.getReleaseFn()(retError)
		}
	}()

	if _, err := targetWorkflow.getContext().persistNonFirstWorkflowEvents(
		targetWorkflowEvents,
	); err != nil {
		return err
	}

	updateMode, transactionPolicy, err := r.backfillWorkflowEventsReapply(
		ctx,
		targetWorkflow,
		targetWorkflowEvents,
	)
	if err != nil {
		return err
	}

	return targetWorkflow.getContext().updateWorkflowExecutionWithNew(
		now,
		updateMode,
		nil,
		nil,
		transactionPolicy,
		nil,
	)
}

func (r *nDCTransactionMgrImpl) backfillWorkflowEventsReapply(
	ctx ctx.Context,
	targetWorkflow nDCWorkflow,
	targetWorkflowEvents *persistence.WorkflowEvents,
) (persistence.UpdateWorkflowMode, transactionPolicy, error) {

	isCurrentWorkflow, err := r.isWorkflowCurrent(ctx, targetWorkflow)
	if err != nil {
		return 0, transactionPolicyActive, err
	}
	isWorkflowRunning := targetWorkflow.getMutableState().IsWorkflowExecutionRunning()
	targetWorkflowActiveCluster := r.clusterMetadata.ClusterNameForFailoverVersion(
		targetWorkflow.getMutableState().GetDomainEntry().GetFailoverVersion(),
	)
	currentCluster := r.clusterMetadata.GetCurrentClusterName()
	isActiveCluster := targetWorkflowActiveCluster == currentCluster

	// workflow events reapplication
	// we need to handle 3 cases
	// 1. target workflow is self & self being current & active
	//  a. workflow still running -> just reapply
	//  b. workflow closed -> reset current workflow & reapply
	// 2. anything not case 1 -> find the current & active workflow to reapply

	// case 1
	if isCurrentWorkflow && isActiveCluster {
		// case 1.a
		if isWorkflowRunning {
			if _, err := r.eventsReapplier.reapplyEvents(
				ctx,
				targetWorkflow.getMutableState(),
				targetWorkflowEvents.Events,
				targetWorkflow.getMutableState().GetExecutionInfo().RunID,
			); err != nil {
				return 0, transactionPolicyActive, err
			}
			return persistence.UpdateWorkflowModeUpdateCurrent, transactionPolicyActive, nil
		}

		// case 1.b
		// need to reset target workflow (which is also the current workflow)
		// to accept events to be reapplied
		baseMutableState := targetWorkflow.getMutableState()
		domainID := baseMutableState.GetExecutionInfo().DomainID
		workflowID := baseMutableState.GetExecutionInfo().WorkflowID
		baseRunID := baseMutableState.GetExecutionInfo().RunID
		resetRunID := uuid.New()
		baseRebuildLastEventID := baseMutableState.GetPreviousStartedEventID()

		// TODO when https://github.com/uber/cadence/issues/2420 is finished, remove this block,
		//  since cannot reapply event to a finished workflow which had no decisions started
		if baseRebuildLastEventID == common.EmptyEventID {
			r.logger.Warn("cannot reapply event to a finished workflow",
				tag.WorkflowDomainID(domainID),
				tag.WorkflowID(workflowID),
			)
			r.metricsClient.IncCounter(metrics.HistoryReapplyEventsScope, metrics.EventReapplySkippedCount)
			return persistence.UpdateWorkflowModeBypassCurrent, transactionPolicyPassive, nil
		}

		baseVersionHistories := baseMutableState.GetVersionHistories()
		baseCurrentVersionHistory, err := baseVersionHistories.GetCurrentVersionHistory()
		if err != nil {
			return 0, transactionPolicyActive, err
		}
		baseRebuildLastEventVersion, err := baseCurrentVersionHistory.GetEventVersion(baseRebuildLastEventID)
		if err != nil {
			return 0, transactionPolicyActive, err
		}
		baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
		baseNextEventID := baseMutableState.GetNextEventID()

		if err = r.workflowResetter.resetWorkflow(
			ctx,
			domainID,
			workflowID,
			baseRunID,
			baseCurrentBranchToken,
			baseRebuildLastEventID,
			baseRebuildLastEventVersion,
			baseNextEventID,
			resetRunID,
			uuid.New(),
			targetWorkflow,
			eventsReapplicationResetWorkflowReason,
			targetWorkflowEvents.Events,
		); err != nil {
			return 0, transactionPolicyActive, err
		}
		// after the reset of target workflow (current workflow) with additional events to be reapplied
		// target workflow is no longer the current workflow
		return persistence.UpdateWorkflowModeBypassCurrent, transactionPolicyPassive, nil
	}

	// case 2
	//  find the current & active workflow to reapply
	if err := targetWorkflow.getContext().reapplyEvents(
		[]*persistence.WorkflowEvents{targetWorkflowEvents},
	); err != nil {
		return 0, transactionPolicyActive, err
	}

	if isCurrentWorkflow {
		return persistence.UpdateWorkflowModeUpdateCurrent, transactionPolicyPassive, nil
	}
	return persistence.UpdateWorkflowModeBypassCurrent, transactionPolicyPassive, nil
}

func (r *nDCTransactionMgrImpl) checkWorkflowExists(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (bool, error) {

	_, err := r.shard.GetExecutionManager().GetWorkflowExecution(
		&persistence.GetWorkflowExecutionRequest{
			DomainID: domainID,
			Execution: shared.WorkflowExecution{
				WorkflowId: common.StringPtr(workflowID),
				RunId:      common.StringPtr(runID),
			},
		},
	)

	switch err.(type) {
	case nil:
		return true, nil
	case *shared.EntityNotExistsError:
		return false, nil
	default:
		return false, err
	}
}

func (r *nDCTransactionMgrImpl) getCurrentWorkflowRunID(
	ctx ctx.Context,
	domainID string,
	workflowID string,
) (string, error) {

	resp, err := r.shard.GetExecutionManager().GetCurrentExecution(
		&persistence.GetCurrentExecutionRequest{
			DomainID:   domainID,
			WorkflowID: workflowID,
		},
	)

	switch err.(type) {
	case nil:
		return resp.RunID, nil
	case *shared.EntityNotExistsError:
		return "", nil
	default:
		return "", err
	}
}

func (r *nDCTransactionMgrImpl) loadNDCWorkflow(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (nDCWorkflow, error) {

	// we need to check the current workflow execution
	context, release, err := r.historyCache.getOrCreateWorkflowExecution(
		ctx,
		domainID,
		shared.WorkflowExecution{
			WorkflowId: common.StringPtr(workflowID),
			RunId:      common.StringPtr(runID),
		},
	)
	if err != nil {
		return nil, err
	}

	msBuilder, err := context.loadWorkflowExecution()
	if err != nil {
		// no matter what error happen, we need to retry
		release(err)
		return nil, err
	}
	return newNDCWorkflow(ctx, r.domainCache, r.clusterMetadata, context, msBuilder, release), nil
}

func (r *nDCTransactionMgrImpl) isWorkflowCurrent(
	ctx ctx.Context,
	targetWorkflow nDCWorkflow,
) (bool, error) {

	// since we are not rebuilding the mutable state (when doing backfill) then we
	// can trust the result from IsCurrentWorkflowGuaranteed
	if targetWorkflow.getMutableState().IsCurrentWorkflowGuaranteed() {
		return true, nil
	}

	// target workflow is not guaranteed to be current workflow, do additional check
	executionInfo := targetWorkflow.getMutableState().GetExecutionInfo()
	domainID := executionInfo.DomainID
	workflowID := executionInfo.WorkflowID
	runID := executionInfo.RunID

	currentRunID, err := r.getCurrentWorkflowRunID(
		ctx,
		domainID,
		workflowID,
	)
	if err != nil {
		return false, err
	}

	return currentRunID == runID, nil
}
