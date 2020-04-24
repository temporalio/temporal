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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination existing_workflow_transaction_manager_mock.go

package ndc

import (
	ctx "context"
	"fmt"
	"time"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/execution"
)

type (
	transactionManagerForExistingWorkflow interface {
		dispatchForExistingWorkflow(
			ctx ctx.Context,
			now time.Time,
			isWorkflowRebuilt bool,
			targetWorkflow execution.Workflow,
			newWorkflow execution.Workflow,
		) error
	}

	transactionManagerForExistingWorkflowImpl struct {
		transactionManager transactionManager
	}
)

var _ transactionManagerForExistingWorkflow = (*transactionManagerForExistingWorkflowImpl)(nil)

func newTransactionManagerForExistingWorkflow(
	transactionManager transactionManager,
) transactionManagerForExistingWorkflow {

	return &transactionManagerForExistingWorkflowImpl{
		transactionManager: transactionManager,
	}
}

func (r *transactionManagerForExistingWorkflowImpl) dispatchForExistingWorkflow(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	// NOTE: this function does NOT mutate current workflow, target workflow or new workflow,
	//  workflow mutation is done in methods within executeTransaction function

	// this is a performance optimization so most update does not need to
	// check whether target workflow is current workflow by calling DB API
	if !isWorkflowRebuilt && targetWorkflow.GetMutableState().IsCurrentWorkflowGuaranteed() {
		// NOTE: if target workflow is rebuilt, then IsCurrentWorkflowGuaranteed is not trustworthy

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			now,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	targetExecutionInfo := targetWorkflow.GetMutableState().GetExecutionInfo()
	domainID := targetExecutionInfo.DomainID
	workflowID := targetExecutionInfo.WorkflowID
	targetRunID := targetExecutionInfo.RunID

	// the target workflow is rebuilt
	// we need to check the current workflow execution
	currentRunID, err := r.transactionManager.getCurrentWorkflowRunID(
		ctx,
		domainID,
		workflowID,
	)
	if err != nil {
		return err
	}
	if currentRunID == "" {
		// this means a bug in our code or DB is inconsistent...
		return &shared.InternalServiceError{
			Message: "nDCTransactionManager: unable to locate current workflow during update",
		}
	}

	if currentRunID == targetRunID {
		if !isWorkflowRebuilt {
			return &shared.InternalServiceError{
				Message: "nDCTransactionManager: encounter workflow not rebuilt & current workflow not guaranteed",
			}
		}

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			now,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := r.transactionManager.loadNDCWorkflow(
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
		return r.dispatchWorkflowUpdateAsZombie(
			ctx,
			now,
			isWorkflowRebuilt,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)
	}

	// isWorkflowRebuilt is irrelevant here, because the DB API to be used
	// will set target workflow using snapshot
	return r.executeTransaction(
		ctx,
		now,
		transactionPolicySuppressCurrentAndUpdateAsCurrent,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) dispatchWorkflowUpdateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			now,
			transactionPolicyUpdateAsCurrent,
			nil,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		now,
		transactionPolicyConflictResolveAsCurrent,
		nil,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) dispatchWorkflowUpdateAsZombie(
	ctx ctx.Context,
	now time.Time,
	isWorkflowRebuilt bool,
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			now,
			transactionPolicyUpdateAsZombie,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)
	}

	return r.executeTransaction(
		ctx,
		now,
		transactionPolicyConflictResolveAsZombie,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) updateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	if newWorkflow == nil {
		return targetWorkflow.GetContext().UpdateWorkflowExecutionAsPassive(now)
	}

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNewAsPassive(
		now,
		newWorkflow.GetContext(),
		newWorkflow.GetMutableState(),
	)
}

func (r *transactionManagerForExistingWorkflowImpl) updateAsZombie(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	targetPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if targetPolicy != execution.TransactionPolicyPassive {
		return &shared.InternalServiceError{
			Message: "nDCTransactionManagerForExistingWorkflow updateAsZombie encounter target workflow policy not being passive",
		}
	}

	var newContext execution.Context
	var newMutableState execution.MutableState
	var newTransactionPolicy *execution.TransactionPolicy
	if newWorkflow != nil {
		newWorkflowPolicy, err := newWorkflow.SuppressBy(
			currentWorkflow,
		)
		if err != nil {
			return err
		}
		if newWorkflowPolicy != execution.TransactionPolicyPassive {
			return &shared.InternalServiceError{
				Message: "nDCTransactionManagerForExistingWorkflow updateAsZombie encounter new workflow policy not being passive",
			}
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newWorkflowExists, err := r.transactionManager.checkWorkflowExists(
			ctx,
			newExecutionInfo.DomainID,
			newExecutionInfo.WorkflowID,
			newExecutionInfo.RunID,
		)
		if err != nil {
			return err
		}
		if newWorkflowExists {
			// new workflow already exists, do not create again
			newContext = nil
			newMutableState = nil
			newTransactionPolicy = nil
		} else {
			// new workflow does not exists, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
			newTransactionPolicy = execution.TransactionPolicyPassive.Ptr()
		}
	}

	// release lock on current workflow, since current cluster maybe the active cluster
	//  and events maybe reapplied to current workflow
	currentWorkflow.GetReleaseFn()(nil)
	currentWorkflow = nil

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		now,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		execution.TransactionPolicyPassive,
		newTransactionPolicy,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) suppressCurrentAndUpdateAsCurrent(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	var err error

	currentWorkflowPolicy := execution.TransactionPolicyPassive
	if currentWorkflow.GetMutableState().IsWorkflowExecutionRunning() {
		currentWorkflowPolicy, err = currentWorkflow.SuppressBy(
			targetWorkflow,
		)
		if err != nil {
			return err
		}
	}
	if err := targetWorkflow.Revive(); err != nil {
		return err
	}

	var newContext execution.Context
	var newMutableState execution.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		if err := newWorkflow.Revive(); err != nil {
			return err
		}
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		currentWorkflow.GetContext(),
		currentWorkflow.GetMutableState(),
		currentWorkflowPolicy.Ptr(),
		nil,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) conflictResolveAsCurrent(
	ctx ctx.Context,
	now time.Time,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	var newContext execution.Context
	var newMutableState execution.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		nil,
		nil,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) conflictResolveAsZombie(
	ctx ctx.Context,
	now time.Time,
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) error {

	targetWorkflowPolicy, err := targetWorkflow.SuppressBy(
		currentWorkflow,
	)
	if err != nil {
		return err
	}
	if targetWorkflowPolicy != execution.TransactionPolicyPassive {
		return &shared.InternalServiceError{
			Message: "nDCTransactionManagerForExistingWorkflow conflictResolveAsZombie encounter target workflow policy not being passive",
		}
	}

	var newContext execution.Context
	var newMutableState execution.MutableState
	if newWorkflow != nil {
		newWorkflowPolicy, err := newWorkflow.SuppressBy(
			currentWorkflow,
		)
		if err != nil {
			return err
		}
		if newWorkflowPolicy != execution.TransactionPolicyPassive {
			return &shared.InternalServiceError{
				Message: "nDCTransactionManagerForExistingWorkflow conflictResolveAsZombie encounter new workflow policy not being passive",
			}
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newWorkflowExists, err := r.transactionManager.checkWorkflowExists(
			ctx,
			newExecutionInfo.DomainID,
			newExecutionInfo.WorkflowID,
			newExecutionInfo.RunID,
		)
		if err != nil {
			return err
		}
		if newWorkflowExists {
			// new workflow already exists, do not create again
			newContext = nil
			newMutableState = nil
		} else {
			// new workflow does not exists, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
		}
	}

	// release lock on current workflow, since current cluster maybe the active cluster
	//  and events maybe reapplied to current workflow
	currentWorkflow.GetReleaseFn()(nil)
	currentWorkflow = nil

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		now,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		nil,
		nil,
	)
}

func (r *transactionManagerForExistingWorkflowImpl) executeTransaction(
	ctx ctx.Context,
	now time.Time,
	transactionPolicy transactionPolicy,
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
) (retError error) {

	defer func() {
		if rec := recover(); rec != nil {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, newWorkflow, errPanic)
			panic(rec)
		} else {
			r.cleanupTransaction(currentWorkflow, targetWorkflow, newWorkflow, retError)
		}
	}()

	switch transactionPolicy {
	case transactionPolicyUpdateAsCurrent:
		return r.updateAsCurrent(
			ctx,
			now,
			targetWorkflow,
			newWorkflow,
		)

	case transactionPolicyUpdateAsZombie:
		return r.updateAsZombie(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case transactionPolicySuppressCurrentAndUpdateAsCurrent:
		return r.suppressCurrentAndUpdateAsCurrent(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case transactionPolicyConflictResolveAsCurrent:
		return r.conflictResolveAsCurrent(
			ctx,
			now,
			targetWorkflow,
			newWorkflow,
		)

	case transactionPolicyConflictResolveAsZombie:
		return r.conflictResolveAsZombie(
			ctx,
			now,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	default:
		return &shared.InternalServiceError{
			Message: fmt.Sprintf("nDCTransactionManager: encounter unknown transaction type: %v", transactionPolicy),
		}
	}
}

func (r *transactionManagerForExistingWorkflowImpl) cleanupTransaction(
	currentWorkflow execution.Workflow,
	targetWorkflow execution.Workflow,
	newWorkflow execution.Workflow,
	err error,
) {

	if currentWorkflow != nil {
		currentWorkflow.GetReleaseFn()(err)
	}
	if targetWorkflow != nil {
		targetWorkflow.GetReleaseFn()(err)
	}
	if newWorkflow != nil {
		newWorkflow.GetReleaseFn()(err)
	}
}
