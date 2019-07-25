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

	"github.com/uber/cadence/.gen/go/shared"
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
	}

	nDCTransactionMgrImpl struct {
		shard           ShardContext
		domainCache     cache.DomainCache
		historyCache    *historyCache
		clusterMetadata cluster.Metadata
		historyV2Mgr    persistence.HistoryV2Manager
		metricsClient   metrics.Client
		logger          log.Logger

		createMgr nDCTransactionMgrForNewWorkflow
		updateMgr nDCTransactionMgrForExistingWorkflow
	}
)

func newNDCTransactionMgr(
	shard ShardContext,
	historyCache *historyCache,
	logger log.Logger,
) *nDCTransactionMgrImpl {

	transactionMgr := &nDCTransactionMgrImpl{
		shard:           shard,
		domainCache:     shard.GetDomainCache(),
		historyCache:    historyCache,
		clusterMetadata: shard.GetService().GetClusterMetadata(),
		historyV2Mgr:    shard.GetHistoryV2Manager(),
		metricsClient:   shard.GetMetricsClient(),
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

	defer func() { targetWorkflow.getReleaseFn()(retError) }()

	if _, err := targetWorkflow.getContext().persistNonFirstWorkflowEvents(
		targetWorkflowEvents,
	); err != nil {
		return err
	}

	// TODO nDC: we need to have the ability to control whether to update
	//  the current record, by `UpdateWorkflowMode`
	return targetWorkflow.getContext().updateWorkflowExecutionWithNew(
		now,
		nil,
		nil,
		transactionPolicyPassive,
		nil,
	)
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
