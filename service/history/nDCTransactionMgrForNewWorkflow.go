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
)

type (
	nDCTransactionMgrForNewWorkflow interface {
		dispatchForNewWorkflow(
			ctx ctx.Context,
			now time.Time,
			targetWorkflow nDCWorkflow,
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
	targetWorkflow nDCWorkflow,
) error {
	// NOTE: this function does NOT mutate current workflow or target workflow,
	//  workflow mutation is done in methods within executeTransaction function

	targetExecutionInfo := targetWorkflow.getMutableState().GetExecutionInfo()
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

	targetWorkflowIsNewer, err := targetWorkflow.happensAfter(currentWorkflow)
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
	if !currentWorkflow.getMutableState().IsWorkflowExecutionRunning() {
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
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
) error {

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.getMutableState().CloseTransactionAsSnapshot(
		now,
		transactionPolicyPassive,
	)
	if err != nil {
		return err
	}

	targetWorkflowHistorySize, err := targetWorkflow.getContext().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	)
	if err != nil {
		return err
	}

	// target workflow to be created as current
	if currentWorkflow != nil {
		// current workflow exists, need to do compare and swap
		createMode := persistence.CreateWorkflowModeWorkflowIDReuse
		prevRunID := currentWorkflow.getMutableState().GetExecutionInfo().RunID
		prevLastWriteVersion, _, err := currentWorkflow.getVectorClock()
		if err != nil {
			return err
		}
		return targetWorkflow.getContext().createWorkflowExecution(
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
	return targetWorkflow.getContext().createWorkflowExecution(
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
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
) error {

	targetWorkflowPolicy, err := targetWorkflow.suppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if targetWorkflowPolicy != transactionPolicyPassive {
		return &shared.InternalServiceError{
			Message: "nDCTransactionMgrForNewWorkflow createAsZombie encounter target workflow policy not being passive",
		}
	}

	targetWorkflowSnapshot, targetWorkflowEventsSeq, err := targetWorkflow.getMutableState().CloseTransactionAsSnapshot(
		now,
		targetWorkflowPolicy,
	)
	if err != nil {
		return err
	}

	targetWorkflowHistorySize, err := targetWorkflow.getContext().persistFirstWorkflowEvents(
		targetWorkflowEventsSeq[0],
	)
	if err != nil {
		return err
	}

	if err := targetWorkflow.getContext().reapplyEvents(
		targetWorkflowEventsSeq,
	); err != nil {
		return err
	}

	createMode := persistence.CreateWorkflowModeZombie
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = targetWorkflow.getContext().createWorkflowExecution(
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
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
) error {

	currentWorkflowPolicy, err := currentWorkflow.suppressBy(
		targetWorkflow,
	)
	if err != nil {
		return err
	}
	if err := targetWorkflow.revive(); err != nil {
		return err
	}

	return currentWorkflow.getContext().updateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeUpdateCurrent,
		targetWorkflow.getContext(),
		targetWorkflow.getMutableState(),
		currentWorkflowPolicy,
		transactionPolicyPassive.ptr(),
	)
}

func (r *nDCTransactionMgrForNewWorkflowImpl) executeTransaction(
	ctx ctx.Context,
	now time.Time,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
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
	currentWorkflow nDCWorkflow,
	targetWorkflow nDCWorkflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.getReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.getReleaseFn()(err)
	}
}
