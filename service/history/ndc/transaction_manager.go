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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transaction_manager_mock.go

package ndc

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
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/reset"
	"github.com/uber/cadence/service/history/shard"
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
// 1. create as current															-> transactionPolicyCreateAsCurrent
// 2. create as zombie															-> transactionPolicyCreateAsZombie
//
// create path (there will be only current branch) + suppress current
// 1. create as current & suppress current										-> transactionPolicySuppressCurrentAndCreateAsCurrent
//
// update to current branch path
// 1. update as current															-> transactionPolicyUpdateAsCurrent
// 2. update as current & new created as current								-> transactionPolicyUpdateAsCurrent
// 3. update as zombie															-> transactionPolicyUpdateAsZombie
// 4. update as zombie & new created as zombie									-> transactionPolicyUpdateAsZombie
//
// backfill to non current branch path
// 1. backfill as is
// 2. backfill as is & new created as zombie
//
// conflict resolve path
// 1. conflict resolve as current												-> transactionPolicyConflictResolveAsCurrent
// 2. conflict resolve as zombie												-> transactionPolicyConflictResolveAsZombie
//
// conflict resolve path + suppress current
// 1. update from zombie to current & suppress current							-> transactionPolicySuppressCurrentAndUpdateAsCurrent
// 2. update from zombie to current & new created as current & suppress current	-> transactionPolicySuppressCurrentAndUpdateAsCurrent

type transactionPolicy int

const (
	transactionPolicyCreateAsCurrent transactionPolicy = iota
	transactionPolicyCreateAsZombie
	transactionPolicySuppressCurrentAndCreateAsCurrent

	transactionPolicyUpdateAsCurrent
	transactionPolicyUpdateAsZombie

	transactionPolicyConflictResolveAsCurrent
	transactionPolicyConflictResolveAsZombie

	transactionPolicySuppressCurrentAndUpdateAsCurrent

	// EventsReapplicationResetWorkflowReason is the reason for reset workflow during reapplication
	EventsReapplicationResetWorkflowReason = "events-reapplication"
)

type (
	transactionManager interface {
		createWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow execution.Workflow,
		) error
		updateWorkflow(
			ctx ctx.Context,
			now time.Time,
			isWorkflowRebuilt bool,
			targetWorkflow execution.Workflow,
			newWorkflow execution.Workflow,
		) error
		backfillWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow execution.Workflow,
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
		) (execution.Workflow, error)
	}

	transactionManagerImpl struct {
		shard            shard.Context
		domainCache      cache.DomainCache
		executionCache   *execution.Cache
		clusterMetadata  cluster.Metadata
		historyV2Manager persistence.HistoryManager
		serializer       persistence.PayloadSerializer
		metricsClient    metrics.Client
		workflowResetter reset.WorkflowResetter
		eventsReapplier  EventsReapplier
		logger           log.Logger

		createManager transactionManagerForNewWorkflow
		updateManager transactionManagerForExistingWorkflow
	}
)

var _ transactionManager = (*transactionManagerImpl)(nil)

func newTransactionManager(
	shard shard.Context,
	executionCache *execution.Cache,
	eventsReapplier EventsReapplier,
	logger log.Logger,
) *transactionManagerImpl {

	transactionManager := &transactionManagerImpl{
		shard:            shard,
		domainCache:      shard.GetDomainCache(),
		executionCache:   executionCache,
		clusterMetadata:  shard.GetClusterMetadata(),
		historyV2Manager: shard.GetHistoryManager(),
		serializer:       shard.GetService().GetPayloadSerializer(),
		metricsClient:    shard.GetMetricsClient(),
		workflowResetter: reset.NewWorkflowResetter(
			shard,
			executionCache,
			logger,
		),
		eventsReapplier: eventsReapplier,
		logger:          logger.WithTags(tag.ComponentHistoryReplicator),

		createManager: nil,
		updateManager: nil,
	}
	transactionManager.createManager = newTransactionManagerForNewWorkflow(transactionManager)
	transactionManager.updateManager = newTransactionManagerForExistingWorkflow(transactionManager)
	return transactionManager
}

func (r *transactionManagerImpl) createWorkflow(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow execution.Workflow,
) error {

	return r.createManager.dispatchForNewWorkflow(
		ctx,
		now,
		targetWorkflow,
	)
}

func (r *transactionManagerImpl) updateWorkflow(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	return r.updateManager.dispatchForExistingWorkflow(
		ctx,
		now,
		isWorkflowRebuilt,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *transactionManagerImpl) backfillWorkflow(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow execution.Workflow,
	targetWorkflowEvents *persistence.WorkflowEvents,
) (retError error) {

	defer func() {
		if rec := recover(); rec != nil {
			targetWorkflow.GetReleaseFn()(errPanic)
			panic(rec)
		} else {
			targetWorkflow.GetReleaseFn()(retError)
		}
	}()

	if _, err := targetWorkflow.GetContext().PersistNonFirstWorkflowEvents(
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

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		now,
		updateMode,
		nil,
		nil,
		transactionPolicy,
		nil,
	)
}

func (r *transactionManagerImpl) backfillWorkflowEventsReapply(
	ctx ctx.Context,
	targetWorkflow execution.Workflow,
	targetWorkflowEvents *persistence.WorkflowEvents,
) (persistence.UpdateWorkflowMode, execution.TransactionPolicy, error) {

	isCurrentWorkflow, err := r.isWorkflowCurrent(ctx, targetWorkflow)
	if err != nil {
		return 0, execution.TransactionPolicyActive, err
	}
	isWorkflowRunning := targetWorkflow.GetMutableState().IsWorkflowExecutionRunning()
	targetWorkflowActiveCluster := r.clusterMetadata.ClusterNameForFailoverVersion(
		targetWorkflow.GetMutableState().GetDomainEntry().GetFailoverVersion(),
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
			if _, err := r.eventsReapplier.ReapplyEvents(
				ctx,
				targetWorkflow.GetMutableState(),
				targetWorkflowEvents.Events,
				targetWorkflow.GetMutableState().GetExecutionInfo().RunID,
			); err != nil {
				return 0, execution.TransactionPolicyActive, err
			}
			return persistence.UpdateWorkflowModeUpdateCurrent, execution.TransactionPolicyActive, nil
		}

		// case 1.b
		// need to reset target workflow (which is also the current workflow)
		// to accept events to be reapplied
		baseMutableState := targetWorkflow.GetMutableState()
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
			return persistence.UpdateWorkflowModeBypassCurrent, execution.TransactionPolicyPassive, nil
		}

		baseVersionHistories := baseMutableState.GetVersionHistories()
		baseCurrentVersionHistory, err := baseVersionHistories.GetCurrentVersionHistory()
		if err != nil {
			return 0, execution.TransactionPolicyActive, err
		}
		baseRebuildLastEventVersion, err := baseCurrentVersionHistory.GetEventVersion(baseRebuildLastEventID)
		if err != nil {
			return 0, execution.TransactionPolicyActive, err
		}
		baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
		baseNextEventID := baseMutableState.GetNextEventID()

		if err = r.workflowResetter.ResetWorkflow(
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
			EventsReapplicationResetWorkflowReason,
			targetWorkflowEvents.Events,
		); err != nil {
			return 0, execution.TransactionPolicyActive, err
		}
		// after the reset of target workflow (current workflow) with additional events to be reapplied
		// target workflow is no longer the current workflow
		return persistence.UpdateWorkflowModeBypassCurrent, execution.TransactionPolicyPassive, nil
	}

	// case 2
	//  find the current & active workflow to reapply
	if err := targetWorkflow.GetContext().ReapplyEvents(
		[]*persistence.WorkflowEvents{targetWorkflowEvents},
	); err != nil {
		return 0, execution.TransactionPolicyActive, err
	}

	if isCurrentWorkflow {
		return persistence.UpdateWorkflowModeUpdateCurrent, execution.TransactionPolicyPassive, nil
	}
	return persistence.UpdateWorkflowModeBypassCurrent, execution.TransactionPolicyPassive, nil
}

func (r *transactionManagerImpl) checkWorkflowExists(
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

func (r *transactionManagerImpl) getCurrentWorkflowRunID(
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

func (r *transactionManagerImpl) loadNDCWorkflow(
	ctx ctx.Context,
	domainID string,
	workflowID string,
	runID string,
) (execution.Workflow, error) {

	// we need to check the current workflow execution
	context, release, err := r.executionCache.GetOrCreateWorkflowExecution(
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

	msBuilder, err := context.LoadWorkflowExecution()
	if err != nil {
		// no matter what error happen, we need to retry
		release(err)
		return nil, err
	}
	return execution.NewWorkflow(ctx, r.domainCache, r.clusterMetadata, context, msBuilder, release), nil
}

func (r *transactionManagerImpl) isWorkflowCurrent(
	ctx ctx.Context,
	targetWorkflow execution.Workflow,
) (bool, error) {

	// since we are not rebuilding the mutable state (when doing backfill) then we
	// can trust the result from IsCurrentWorkflowGuaranteed
	if targetWorkflow.GetMutableState().IsCurrentWorkflowGuaranteed() {
		return true, nil
	}

	// target workflow is not guaranteed to be current workflow, do additional check
	executionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
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
