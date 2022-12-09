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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transaction_manager_mock.go

package ndc

import (
	"context"
	"time"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
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
	EventsReapplicationResetWorkflowReason = "events-reapplication"
)

type (
	transactionMgr interface {
		createWorkflow(
			ctx context.Context,
			now time.Time,
			targetWorkflow Workflow,
		) error
		updateWorkflow(
			ctx context.Context,
			now time.Time,
			isWorkflowRebuilt bool,
			targetWorkflow Workflow,
			newWorkflow Workflow,
		) error
		backfillWorkflow(
			ctx context.Context,
			now time.Time,
			targetWorkflow Workflow,
			targetWorkflowEvents *persistence.WorkflowEvents,
		) error

		checkWorkflowExists(
			ctx context.Context,
			namespaceID namespace.ID,
			workflowID string,
			runID string,
		) (bool, error)
		getCurrentWorkflowRunID(
			ctx context.Context,
			namespaceID namespace.ID,
			workflowID string,
		) (string, error)
		loadWorkflow(
			ctx context.Context,
			namespaceID namespace.ID,
			workflowID string,
			runID string,
		) (Workflow, error)
	}

	transactionMgrImpl struct {
		shard             shard.Context
		namespaceRegistry namespace.Registry
		workflowCache     wcache.Cache
		clusterMetadata   cluster.Metadata
		executionManager  persistence.ExecutionManager
		serializer        serialization.Serializer
		metricsHandler    metrics.MetricsHandler
		workflowResetter  WorkflowResetter
		eventsReapplier   EventsReapplier
		logger            log.Logger

		createMgr transactionMgrForNewWorkflow
		updateMgr transactionMgrForExistingWorkflow
	}
)

var _ transactionMgr = (*transactionMgrImpl)(nil)

func newTransactionMgr(
	shard shard.Context,
	workflowCache wcache.Cache,
	eventsReapplier EventsReapplier,
	logger log.Logger,
) *transactionMgrImpl {

	transactionMgr := &transactionMgrImpl{
		shard:             shard,
		namespaceRegistry: shard.GetNamespaceRegistry(),
		workflowCache:     workflowCache,
		clusterMetadata:   shard.GetClusterMetadata(),
		executionManager:  shard.GetExecutionManager(),
		serializer:        shard.GetPayloadSerializer(),
		metricsHandler:    shard.GetMetricsHandler(),
		workflowResetter: NewWorkflowResetter(
			shard,
			workflowCache,
			logger,
		),
		eventsReapplier: eventsReapplier,
		logger:          log.With(logger, tag.ComponentHistoryReplicator),

		createMgr: nil,
		updateMgr: nil,
	}
	transactionMgr.createMgr = newTransactionMgrForNewWorkflow(transactionMgr)
	transactionMgr.updateMgr = newNDCTransactionMgrForExistingWorkflow(transactionMgr)
	return transactionMgr
}

func (r *transactionMgrImpl) createWorkflow(
	ctx context.Context,
	now time.Time,
	targetWorkflow Workflow,
) error {

	return r.createMgr.dispatchForNewWorkflow(
		ctx,
		now,
		targetWorkflow,
	)
}

func (r *transactionMgrImpl) updateWorkflow(
	ctx context.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	return r.updateMgr.dispatchForExistingWorkflow(
		ctx,
		now,
		isWorkflowRebuilt,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *transactionMgrImpl) backfillWorkflow(
	ctx context.Context,
	now time.Time,
	targetWorkflow Workflow,
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

	if _, err := targetWorkflow.GetContext().PersistWorkflowEvents(
		ctx,
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
		ctx,
		now,
		updateMode,
		nil,
		nil,
		transactionPolicy,
		nil,
	)
}

func (r *transactionMgrImpl) backfillWorkflowEventsReapply(
	ctx context.Context,
	targetWorkflow Workflow,
	targetWorkflowEvents *persistence.WorkflowEvents,
) (persistence.UpdateWorkflowMode, workflow.TransactionPolicy, error) {

	isCurrentWorkflow, err := r.isWorkflowCurrent(ctx, targetWorkflow)
	if err != nil {
		return 0, workflow.TransactionPolicyActive, err
	}
	isWorkflowRunning := targetWorkflow.GetMutableState().IsWorkflowExecutionRunning()
	targetWorkflowActiveCluster := targetWorkflow.GetMutableState().GetNamespaceEntry().ActiveClusterName()
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
				targetWorkflow.GetMutableState().GetExecutionState().GetRunId(),
			); err != nil {
				return 0, workflow.TransactionPolicyActive, err
			}
			return persistence.UpdateWorkflowModeUpdateCurrent, workflow.TransactionPolicyActive, nil
		}

		// case 1.b
		// need to reset target workflow (which is also the current workflow)
		// to accept events to be reapplied
		baseMutableState := targetWorkflow.GetMutableState()
		namespaceID := namespace.ID(baseMutableState.GetExecutionInfo().NamespaceId)
		workflowID := baseMutableState.GetExecutionInfo().WorkflowId
		baseRunID := baseMutableState.GetExecutionState().GetRunId()
		resetRunID := uuid.New()
		baseRebuildLastEventID := baseMutableState.GetPreviousStartedEventID()
		baseVersionHistories := baseMutableState.GetExecutionInfo().GetVersionHistories()
		baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
		if err != nil {
			return 0, workflow.TransactionPolicyActive, err
		}
		baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
		if err != nil {
			return 0, workflow.TransactionPolicyActive, err
		}
		baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
		baseNextEventID := baseMutableState.GetNextEventID()

		err = r.workflowResetter.ResetWorkflow(
			ctx,
			namespaceID,
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
			enumspb.RESET_REAPPLY_TYPE_SIGNAL,
		)
		switch err.(type) {
		case *serviceerror.InvalidArgument:
			// no-op. Usually this is due to reset workflow with pending child workflows
			r.logger.Warn("Cannot reset workflow. Ignoring reapply events.", tag.Error(err))
			// the target workflow is not reset so it is still the current workflow. It need to persist updated version histories.
			return persistence.UpdateWorkflowModeUpdateCurrent, workflow.TransactionPolicyPassive, nil
		case nil:
			// after the reset of target workflow (current workflow) with additional events to be reapplied
			// target workflow is no longer the current workflow
			return persistence.UpdateWorkflowModeBypassCurrent, workflow.TransactionPolicyPassive, nil
		default:
			return 0, workflow.TransactionPolicyActive, err
		}
	}

	// case 2
	//  find the current & active workflow to reapply
	if err := targetWorkflow.GetContext().ReapplyEvents(
		[]*persistence.WorkflowEvents{targetWorkflowEvents},
	); err != nil {
		return 0, workflow.TransactionPolicyActive, err
	}

	if isCurrentWorkflow {
		return persistence.UpdateWorkflowModeUpdateCurrent, workflow.TransactionPolicyPassive, nil
	}
	return persistence.UpdateWorkflowModeBypassCurrent, workflow.TransactionPolicyPassive, nil
}

func (r *transactionMgrImpl) checkWorkflowExists(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
) (bool, error) {

	_, err := r.shard.GetWorkflowExecution(
		ctx,
		&persistence.GetWorkflowExecutionRequest{
			ShardID:     r.shard.GetShardID(),
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
			RunID:       runID,
		},
	)

	switch err.(type) {
	case nil:
		return true, nil
	case *serviceerror.NotFound:
		return false, nil
	default:
		return false, err
	}
}

func (r *transactionMgrImpl) getCurrentWorkflowRunID(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
) (string, error) {

	resp, err := r.shard.GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     r.shard.GetShardID(),
			NamespaceID: namespaceID.String(),
			WorkflowID:  workflowID,
		},
	)

	switch err.(type) {
	case nil:
		return resp.RunID, nil
	case *serviceerror.NotFound:
		return "", nil
	default:
		return "", err
	}
}

func (r *transactionMgrImpl) loadWorkflow(
	ctx context.Context,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
) (Workflow, error) {

	// we need to check the current workflow execution
	weContext, release, err := r.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}

	ms, err := weContext.LoadMutableState(ctx)
	if err != nil {
		// no matter what error happen, we need to retry
		release(err)
		return nil, err
	}
	return NewWorkflow(ctx, r.namespaceRegistry, r.clusterMetadata, weContext, ms, release), nil
}

func (r *transactionMgrImpl) isWorkflowCurrent(
	ctx context.Context,
	targetWorkflow Workflow,
) (bool, error) {

	// since we are not rebuilding the mutable state (when doing back fill) then we
	// can trust the result from IsCurrentWorkflowGuaranteed
	if targetWorkflow.GetMutableState().IsCurrentWorkflowGuaranteed() {
		return true, nil
	}

	// target workflow is not guaranteed to be current workflow, do additional check
	executionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
	executionState := targetWorkflow.GetMutableState().GetExecutionState()
	namespaceID := namespace.ID(executionInfo.NamespaceId)
	workflowID := executionInfo.WorkflowId
	runID := executionState.RunId

	currentRunID, err := r.getCurrentWorkflowRunID(
		ctx,
		namespaceID,
		workflowID,
	)
	if err != nil {
		return false, err
	}

	return currentRunID == runID, nil
}
