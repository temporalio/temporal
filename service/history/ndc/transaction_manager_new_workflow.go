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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination transaction_manager_new_workflow_mock.go

package ndc

import (
	"context"
	"fmt"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	transactionMgrForNewWorkflow interface {
		dispatchForNewWorkflow(
			ctx context.Context,
			targetWorkflow Workflow,
		) error
	}

	nDCTransactionMgrForNewWorkflowImpl struct {
		shardContext                shard.Context
		transactionMgr              TransactionManager
		bypassVersionSemanticsCheck bool
	}
)

var _ transactionMgrForNewWorkflow = (*nDCTransactionMgrForNewWorkflowImpl)(nil)

func newTransactionMgrForNewWorkflow(
	shardContext shard.Context,
	transactionMgr TransactionManager,
	bypassVersionSemanticsCheck bool,
) *nDCTransactionMgrForNewWorkflowImpl {

	return &nDCTransactionMgrForNewWorkflowImpl{
		shardContext:                shardContext,
		transactionMgr:              transactionMgr,
		bypassVersionSemanticsCheck: bypassVersionSemanticsCheck,
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) dispatchForNewWorkflow(
	ctx context.Context,
	targetWorkflow Workflow,
) error {

	// NOTE: this function does NOT mutate current workflow or target workflow,
	//  workflow mutation is done in methods within executeTransaction function

	targetExecutionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
	targetExecutionState := targetWorkflow.GetMutableState().GetExecutionState()
	namespaceID := namespace.ID(targetExecutionInfo.NamespaceId)
	workflowID := targetExecutionInfo.WorkflowId
	targetRunID := targetExecutionState.RunId

	// we need to check the current workflow execution
	currentRunID, err := r.transactionMgr.GetCurrentWorkflowRunID(
		ctx,
		namespaceID,
		workflowID,
	)
	if err != nil || currentRunID == targetRunID {
		// error out or workflow already created
		return err
	}

	if currentRunID == "" {
		// current record does not exists
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyCreateAsCurrent,
			nil,
			targetWorkflow,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := r.transactionMgr.LoadWorkflow(
		ctx,
		namespaceID,
		workflowID,
		currentRunID,
	)
	if err != nil {
		return err
	}

	targetWorkflowIsNewer, err := targetWorkflow.HappensAfter(currentWorkflow)
	if err != nil {
		return err
	}

	if !targetWorkflowIsNewer {
		// target workflow is older than current workflow, need to suppress the target workflow
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyCreateAsZombie,
			currentWorkflow,
			targetWorkflow,
		)
	}

	// target workflow is newer than current workflow
	if !currentWorkflow.GetMutableState().IsWorkflowExecutionRunning() {
		// current workflow is completed
		// proceed to create workflow
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyCreateAsCurrent,
			currentWorkflow,
			targetWorkflow,
		)
	}

	// current workflow is still running, need to suppress the current workflow
	return r.executeTransaction(
		ctx,
		nDCTransactionPolicySuppressCurrentAndCreateAsCurrent,
		currentWorkflow,
		targetWorkflow,
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) createAsCurrent(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
) error {

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		workflow.TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}

	// target workflow to be created as current
	if currentWorkflow != nil {
		// current workflow exists, need to do compare and swap
		createMode := persistence.CreateWorkflowModeUpdateCurrent
		prevRunID := currentWorkflow.GetMutableState().GetExecutionState().GetRunId()
		prevLastWriteVersion, _, err := currentWorkflow.GetVectorClock()
		if err != nil {
			return err
		}
		return targetWorkflow.GetContext().CreateWorkflowExecution(
			ctx,
			r.shardContext,
			createMode,
			prevRunID,
			prevLastWriteVersion,
			targetWorkflow.GetMutableState(),
			targetWorkflowSnapshot,
			targetWorkflowEventsSeq,
		)
	}

	// current workflow does not exists, create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	return targetWorkflow.GetContext().CreateWorkflowExecution(
		ctx,
		r.shardContext,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		targetWorkflow.GetMutableState(),
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) createAsZombie(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
) error {

	targetWorkflowPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if !r.bypassVersionSemanticsCheck && targetWorkflowPolicy != workflow.TransactionPolicyPassive {
		return serviceerror.NewInternal("transactionMgrForNewWorkflow createAsZombie encountered target workflow policy not being passive")
	}

	// release lock on current workflow, since current cluster maybe the active cluster
	// and events maybe reapplied to current workflow
	currentWorkflow.GetReleaseFn()(nil)
	currentWorkflow = nil

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		targetWorkflowPolicy,
	)
	if err != nil {
		return err
	}

	if err := targetWorkflow.GetContext().ReapplyEvents(
		ctx,
		r.shardContext,
		targetWorkflowEventsSeq,
	); err != nil {
		return err
	}

	// target workflow is in zombie state, no need to update current record.
	createMode := persistence.CreateWorkflowModeBypassCurrent
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = targetWorkflow.GetContext().CreateWorkflowExecution(
		ctx,
		r.shardContext,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		targetWorkflow.GetMutableState(),
		targetWorkflowSnapshot,
		targetWorkflowEventsSeq,
	)
	switch err.(type) {
	case nil:
		return nil
	case *persistence.WorkflowConditionFailedError:
		// workflow already created
		return nil
	default:
		return err
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) suppressCurrentAndCreateAsCurrent(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
) error {

	currentWorkflowPolicy, err := currentWorkflow.SuppressBy(
		targetWorkflow,
	)
	if err != nil {
		return err
	}
	if err := targetWorkflow.Revive(); err != nil {
		return err
	}

	return currentWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetWorkflow.GetContext(),
		targetWorkflow.GetMutableState(),
		currentWorkflowPolicy,
		workflow.TransactionPolicyPassive.Ptr(),
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) executeTransaction(
	ctx context.Context,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
) (retError error) {

	defer func() {
		if rec := recover(); rec != nil {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, errPanic)
			panic(rec)
		} else {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, retError)
		}
	}()

	switch transactionPolicy {
	case nDCTransactionPolicyCreateAsCurrent:
		return r.createAsCurrent(
			ctx,
			currentWorkflow,
			targetWorkflow,
		)

	case nDCTransactionPolicyCreateAsZombie:
		return r.createAsZombie(
			ctx,
			currentWorkflow,
			targetWorkflow,
		)

	case nDCTransactionPolicySuppressCurrentAndCreateAsCurrent:
		return r.suppressCurrentAndCreateAsCurrent(
			ctx,
			currentWorkflow,
			targetWorkflow,
		)

	default:
		return serviceerror.NewInternal(fmt.Sprintf("transactionMgr: encountered unknown transaction type: %v", transactionPolicy))
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) cleanupTransaction(
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.GetReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.GetReleaseFn()(err)
	}
}
