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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCTransactionMgrForNewWorkflow_mock.go

package history

import (
	ctx "context"
	"fmt"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
	"github.com/uber/cadence/service/history/ndc"
)

type (
	nDCTransactionMgrForNewWorkflow interface {
		dispatchForNewWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow ndc.Workflow,
		) error
	}

	nDCTransactionMgrForNewWorkflowImpl struct {
		transactionMgr nDCTransactionMgr
	}
)

var _ nDCTransactionMgrForNewWorkflow = (*nDCTransactionMgrForNewWorkflowImpl)(nil)

func newNDCTransactionMgrForNewWorkflow(
	transactionMgr nDCTransactionMgr,
) *nDCTransactionMgrForNewWorkflowImpl {

	return &nDCTransactionMgrForNewWorkflowImpl{
		transactionMgr: transactionMgr,
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) dispatchForNewWorkflow(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow ndc.Workflow,
) error {
	// NOTE: this function does NOT mutate current workflow or target workflow,
	//  workflow mutation is done in methods within executeTransaction function

	targetExecutionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
	domainID := targetExecutionInfo.DomainID
	workflowID := targetExecutionInfo.WorkflowID
	targetRunID := targetExecutionInfo.RunID

	// we need to check the current workflow execution
	currentRunID, err := r.transactionMgr.getCurrentWorkflowRunID(
		ctx,
		domainID,
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
			now,
			nDCTransactionPolicyCreateAsCurrent,
			nil,
			targetWorkflow,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := r.transactionMgr.loadNDCWorkflow(
		ctx,
		domainID,
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
			now,
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
			now,
			nDCTransactionPolicyCreateAsCurrent,
			currentWorkflow,
			targetWorkflow,
		)
	}

	// current workflow is still running, need to suppress the current workflow
	return r.executeTransaction(
		ctx,
		now,
		nDCTransactionPolicySuppressCurrentAndCreateAsCurrent,
		currentWorkflow,
		targetWorkflow,
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) createAsCurrent(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow ndc.Workflow,
	targetWorkflow ndc.Workflow,
) error {

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		now,
		execution.TransactionPolicyPassive,
	)
	if err != nil {
		return err
	}

	targetWorkflowHistorySize, err := targetWorkflow.GetContext().PersistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	)
	if err != nil {
		return err
	}

	// target workflow to be created as current
	if currentWorkflow != nil {
		// current workflow exists, need to do compare and swap
		createMode := persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID := currentWorkflow.GetMutableState().GetExecutionInfo().RunID
		prevLastWriteVersion, _, err := currentWorkflow.GetVectorClock()
		if err != nil {
			return err
		}
		return targetWorkflow.GetContext().CreateWorkflowExecution(
			targetWorkflowSnapshot,
			targetWorkflowHistorySize,
			now,
			createMode,
			prevRunID,
			prevLastWriteVersion,
		)
	}

	// current workflow does not exists, create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	return targetWorkflow.GetContext().CreateWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		createMode,
		prevRunID,
		prevLastWriteVersion,
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) createAsZombie(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow ndc.Workflow,
	targetWorkflow ndc.Workflow,
) error {

	targetWorkflowPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if targetWorkflowPolicy != execution.TransactionPolicyPassive {
		return &shared.InternalServiceError{
			Message: "nDCTransactionMgrForNewWorkflow createAsZombie encounter target workflow policy not being passive",
		}
	}

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.GetMutableState().CloseTransactionAsSnapshot(
		now,
		targetWorkflowPolicy,
	)
	if err != nil {
		return err
	}

	targetWorkflowHistorySize, err := targetWorkflow.GetContext().PersistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	)
	if err != nil {
		return err
	}

	if err := targetWorkflow.GetContext().ReapplyEvents(
		targetWorkflowEventsSeq,
	); err != nil {
		return err
	}

	createMode := persistence.CreateWorkflowModeZombie
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = targetWorkflow.GetContext().CreateWorkflowExecution(
		targetWorkflowSnapshot,
		targetWorkflowHistorySize,
		now,
		createMode,
		prevRunID,
		prevLastWriteVersion,
	)
	switch err.(type) {
	case nil:
		return nil
	case *persistence.WorkflowExecutionAlreadyStartedError:
		// workflow already created
		return nil
	default:
		return err
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) suppressCurrentAndCreateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow ndc.Workflow,
	targetWorkflow ndc.Workflow,
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
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetWorkflow.GetContext(),
		targetWorkflow.GetMutableState(),
		currentWorkflowPolicy,
		execution.TransactionPolicyPassive.Ptr(),
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) executeTransaction(
	ctx ctx.Context,
	now time.Time,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow ndc.Workflow,
	targetWorkflow ndc.Workflow,
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
			now,
			currentWorkflow,
			targetWorkflow,
		)

	case nDCTransactionPolicyCreateAsZombie:
		return r.createAsZombie(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
		)

	case nDCTransactionPolicySuppressCurrentAndCreateAsCurrent:
		return r.suppressCurrentAndCreateAsCurrent(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
		)

	default:
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("nDCTransactionMgr: encounter unknown transaction type: %v", transactionPolicy),
		}
	}
}

func (r *nDCTransactionMgrForNewWorkflowImpl) cleanupTransaction(
	currentWorkflow ndc.Workflow,
	targetWorkflow ndc.Workflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.GetReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.GetReleaseFn()(err)
	}
}
