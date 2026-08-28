//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination transaction_manager_existing_workflow_mock.go

package ndc

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/workflow"
)

type (
	transactionMgrForExistingWorkflow interface {
		dispatchForExistingWorkflow(
			ctx context.Context,
			isWorkflowRebuilt bool,
			archetypeID chasm.ArchetypeID,
			targetWorkflow Workflow,
			newWorkflow Workflow,
		) error
	}

	nDCTransactionMgrForExistingWorkflowImpl struct {
		shardContext                historyi.ShardContext
		transactionMgr              TransactionManager
		bypassVersionSemanticsCheck bool
		taskRefresher               workflow.TaskRefresher
	}
)

var _ transactionMgrForExistingWorkflow = (*nDCTransactionMgrForExistingWorkflowImpl)(nil)

func newNDCTransactionMgrForExistingWorkflow(
	shardContext historyi.ShardContext,
	transactionMgr TransactionManager,
	bypassVersionSemanticsCheck bool,
	taskRefresher workflow.TaskRefresher,
) *nDCTransactionMgrForExistingWorkflowImpl {

	return &nDCTransactionMgrForExistingWorkflowImpl{
		shardContext:                shardContext,
		transactionMgr:              transactionMgr,
		bypassVersionSemanticsCheck: bypassVersionSemanticsCheck,
		taskRefresher:               taskRefresher,
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchForExistingWorkflow(
	ctx context.Context,
	isWorkflowRebuilt bool,
	archetypeID chasm.ArchetypeID,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	mutableState := targetWorkflow.GetMutableState()

	// NOTE: this function does NOT mutate current workflow, target workflow or new workflow,
	//  workflow mutation is done in methods within executeTransaction function

	// this is a performance optimization so most update does not need to
	// check whether target workflow is current workflow by calling DB API
	if !isWorkflowRebuilt && mutableState.IsCurrentWorkflowGuaranteed() {
		// NOTE: if target workflow is rebuilt, then IsCurrentWorkflowGuaranteed is not trustworthy

		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	targetExecutionInfo := mutableState.GetExecutionInfo()
	targetExecutionState := mutableState.GetExecutionState()
	namespaceID := namespace.ID(targetExecutionInfo.NamespaceId)
	workflowID := targetExecutionInfo.WorkflowId
	targetRunID := targetExecutionState.RunId

	// the target workflow is rebuilt
	// we need to check the current workflow execution
	currentRunID, err := r.transactionMgr.GetCurrentWorkflowRunID(
		ctx,
		namespaceID,
		workflowID,
		archetypeID,
	)
	if err != nil {
		return err
	}
	if currentRunID == "" {
		// The current execution record is missing. It is written atomically with the run, so this only
		// happens when delete replication removed it (the workflow is being deleted). We must not
		// re-establish the current record here - that would resurrect a deleted workflow - so the target
		// is persisted bypass-current:
		//   - running target: a non-current run is never running, so this is an anomaly -> error.
		//   - not-running target (closed, or already a zombie - IsWorkflowExecutionRunning() is false
		//     for both): bypass-current, leaving the current record missing.
		// A carried newWorkflow (continue-as-new/cron/retry successor) still holds real history we must
		// not drop, so it is passed through to the zombie path: it is persisted bypass-current (as a
		// zombie) when not already present locally, and skipped when it is. It never becomes the current
		// run; if it is truly the live head of the lineage, it is reconciled by its own replication.
		if mutableState.IsWorkflowExecutionRunning() {
			if newWorkflow != nil {
				newWorkflow.GetReleaseFn()(nil)
			}
			return serviceerror.NewInternalf(
				"dispatchForExistingWorkflow: run %v is running but its current execution record is missing (workflow %v)",
				targetRunID,
				workflowID,
			)
		}
		r.shardContext.GetThrottledLogger().Warn(
			"Applying replication update as zombie (bypass-current) for closed run with no current execution; workflow appears deleted",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(targetRunID),
		)
		return r.dispatchWorkflowUpdateAsZombie(
			ctx,
			isWorkflowRebuilt,
			nil, // no current workflow: the current run was deleted
			targetWorkflow,
			newWorkflow, // persisted bypass-current if not already present; never becomes current
			archetypeID,
		)
	}

	if currentRunID == targetRunID {
		// update to current record, since target workflow is pointed by current record
		return r.dispatchWorkflowUpdateAsCurrent(
			ctx,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	// there exists a current workflow, need additional check
	currentWorkflow, err := r.transactionMgr.LoadWorkflow(
		ctx,
		namespaceID,
		workflowID,
		currentRunID,
		archetypeID,
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
			isWorkflowRebuilt,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	// isWorkflowRebuilt is irrelevant here, because the DB API to be used
	// will set target workflow using snapshot
	return r.executeTransaction(
		ctx,
		nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
		archetypeID,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsCurrent(
	ctx context.Context,
	isWorkflowRebuilt bool,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyUpdateAsCurrent,
			nil,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	return r.executeTransaction(
		ctx,
		nDCTransactionPolicyConflictResolveAsCurrent,
		nil,
		targetWorkflow,
		newWorkflow,
		archetypeID,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsZombie(
	ctx context.Context,
	isWorkflowRebuilt bool,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyUpdateAsZombie,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	return r.executeTransaction(
		ctx,
		nDCTransactionPolicyConflictResolveAsZombie,
		currentWorkflow,
		targetWorkflow,
		newWorkflow,
		archetypeID,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsCurrent(
	ctx context.Context,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	if newWorkflow == nil {
		return targetWorkflow.GetContext().UpdateWorkflowExecutionAsPassive(ctx, r.shardContext)
	}

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNewAsPassive(
		ctx,
		r.shardContext,
		newWorkflow.GetContext(),
		newWorkflow.GetMutableState(),
	)
}

// suppressTargetPolicy suppresses the target workflow by the current workflow and returns the
// resulting transaction policy.
//
// It exists solely for the deleted-current-run (orphan) path in this file, where currentWorkflow is
// nil because the current execution record is gone. In that case there is nothing to suppress
// against and the target is guaranteed not running - closed or already a zombie
// (dispatchForExistingWorkflow only reaches the zombie path for a non-running target), so it stays
// passive. Do NOT reuse this elsewhere or call it with a
// nil current for a running target: it would report the target as suppressed without zombifying it.
// Everywhere else, call targetWorkflow.SuppressBy(currentWorkflow) directly.
func suppressTargetPolicy(targetWorkflow Workflow, currentWorkflow Workflow) (historyi.TransactionPolicy, error) {
	if currentWorkflow == nil {
		return historyi.TransactionPolicyPassive, nil
	}
	return targetWorkflow.SuppressBy(currentWorkflow)
}

// suppressNewWorkflowPolicy suppresses the carried new run (continue-as-new/cron/retry successor) by
// the current workflow and returns the resulting transaction policy. It mirrors suppressTargetPolicy
// for the deleted-current-run (orphan) path, where currentWorkflow is nil because the current
// execution record is gone.
//
// With a real current workflow it defers to newWorkflow.SuppressBy(currentWorkflow). With no current
// to compare against, the new run cannot be the current run, so a still-running successor is forced
// into the zombie state (a closed one is left as is) before the caller persists it bypass-current. On
// the passive apply path the successor is always remote-active, so zombie (never terminate) is the
// correct suppression, matching SuppressBy's remote-active branch.
func suppressNewWorkflowPolicy(newWorkflow Workflow, currentWorkflow Workflow) (historyi.TransactionPolicy, error) {
	if currentWorkflow != nil {
		return newWorkflow.SuppressBy(currentWorkflow)
	}
	newMutableState := newWorkflow.GetMutableState()
	if newMutableState.IsWorkflowExecutionRunning() {
		if _, err := newMutableState.UpdateWorkflowStateStatus(
			enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
			enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		); err != nil {
			// The policy is unused on error (callers return immediately); return the conservative
			// passive policy rather than active so a leaked value can never be mistaken for current.
			return historyi.TransactionPolicyPassive, err
		}
	}
	return historyi.TransactionPolicyPassive, nil
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsZombie(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
) error {

	targetPolicy, err := suppressTargetPolicy(targetWorkflow, currentWorkflow)
	if err != nil {
		return err
	}
	if !r.bypassVersionSemanticsCheck && targetPolicy != historyi.TransactionPolicyPassive {
		return serviceerror.NewInternal("transactionMgrForExistingWorkflow updateAsZombie encountered target workflow policy not being passive")
	}

	var newContext historyi.WorkflowContext
	var newMutableState historyi.MutableState
	var newTransactionPolicy *historyi.TransactionPolicy
	if newWorkflow != nil {
		// currentWorkflow is nil on the deleted-current-run (orphan) path; suppressNewWorkflowPolicy
		// handles that by parking the successor as a zombie instead of suppressing against a current.
		newWorkflowPolicy, err := suppressNewWorkflowPolicy(newWorkflow, currentWorkflow)
		if err != nil {
			return err
		}
		if !r.bypassVersionSemanticsCheck && newWorkflowPolicy != historyi.TransactionPolicyPassive {
			return serviceerror.NewInternal("transactionMgrForExistingWorkflow updateAsZombie encountered new workflow policy not being passive")
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newExecutionState := newWorkflow.GetMutableState().GetExecutionState()
		newWorkflowExists, err := r.transactionMgr.CheckWorkflowExists(
			ctx,
			namespace.ID(newExecutionInfo.NamespaceId),
			newExecutionInfo.WorkflowId,
			newExecutionState.RunId,
			archetypeID,
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
			// new workflow does not exist, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
			newTransactionPolicy = historyi.TransactionPolicyPassive.Ptr()
		}
	}

	if currentWorkflow != nil {
		// release lock on current workflow, since current cluster maybe the active cluster
		//  and events maybe reapplied to current workflow
		currentWorkflow.GetReleaseFn()(nil)
		currentWorkflow = nil
	}

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		persistence.UpdateWorkflowModeBypassCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		newTransactionPolicy,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) suppressCurrentAndUpdateAsCurrent(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	var err error
	resetWorkflowPolicy := historyi.TransactionPolicyPassive
	currentWorkflowPolicy := historyi.TransactionPolicyPassive
	if currentWorkflow.GetMutableState().IsWorkflowExecutionRunning() {
		currentWorkflowPolicy, err = currentWorkflow.SuppressBy(
			targetWorkflow,
		)
		if err != nil {
			return err
		}
	}
	if err := targetWorkflow.Revive(ctx, r.taskRefresher); err != nil {
		return err
	}

	var newWorkflowPolicy *historyi.TransactionPolicy
	var newContext historyi.WorkflowContext
	var newMutableState historyi.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		if err := newWorkflow.Revive(ctx, r.taskRefresher); err != nil {
			return err
		}
		newWorkflowPolicy = historyi.TransactionPolicyPassive.Ptr()
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		currentWorkflow.GetContext(),
		currentWorkflow.GetMutableState(),
		resetWorkflowPolicy,
		newWorkflowPolicy,
		currentWorkflowPolicy.Ptr(),
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsCurrent(
	ctx context.Context,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	targetWorkflowPolicy := historyi.TransactionPolicyPassive

	var newWorkflowPolicy *historyi.TransactionPolicy
	var newContext historyi.WorkflowContext
	var newMutableState historyi.MutableState
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		newWorkflowPolicy = historyi.TransactionPolicyPassive.Ptr()
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeUpdateCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		targetWorkflowPolicy,
		newWorkflowPolicy,
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsZombie(
	ctx context.Context,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
) error {

	targetWorkflowPolicy, err := suppressTargetPolicy(targetWorkflow, currentWorkflow)
	if err != nil {
		return err
	}
	if !r.bypassVersionSemanticsCheck && targetWorkflowPolicy != historyi.TransactionPolicyPassive {
		return serviceerror.NewInternal("transactionMgrForExistingWorkflow conflictResolveAsZombie encountered target workflow policy not being passive")
	}

	var newWorkflowPolicy historyi.TransactionPolicy
	var newContext historyi.WorkflowContext
	var newMutableState historyi.MutableState
	if newWorkflow != nil {
		// currentWorkflow is nil on the deleted-current-run (orphan) path; suppressNewWorkflowPolicy
		// handles that by parking the successor as a zombie instead of suppressing against a current.
		newWorkflowPolicy, err = suppressNewWorkflowPolicy(newWorkflow, currentWorkflow)
		if err != nil {
			return err
		}
		if !r.bypassVersionSemanticsCheck && newWorkflowPolicy != historyi.TransactionPolicyPassive {
			return serviceerror.NewInternal("transactionMgrForExistingWorkflow conflictResolveAsZombie encountered new workflow policy not being passive")
		}

		// sanity check if new workflow is already created
		// since workflow resend can have already created the new workflow
		newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
		newExecutionState := newWorkflow.GetMutableState().GetExecutionState()
		newWorkflowExists, err := r.transactionMgr.CheckWorkflowExists(
			ctx,
			namespace.ID(newExecutionInfo.NamespaceId),
			newExecutionInfo.WorkflowId,
			newExecutionState.RunId,
			archetypeID,
		)
		if err != nil {
			return err
		}
		if newWorkflowExists {
			// new workflow already exists, do not create again
			newContext = nil
			newMutableState = nil
		} else {
			// new workflow does not exist, continue
			newContext = newWorkflow.GetContext()
			newMutableState = newWorkflow.GetMutableState()
		}
	}

	if currentWorkflow != nil {
		// release lock on current workflow, since current cluster maybe the active cluster
		//  and events maybe reapplied to current workflow
		currentWorkflow.GetReleaseFn()(nil)
		currentWorkflow = nil
	}

	return targetWorkflow.GetContext().ConflictResolveWorkflowExecution(
		ctx,
		r.shardContext,
		persistence.ConflictResolveWorkflowModeBypassCurrent,
		targetWorkflow.GetMutableState(),
		newContext,
		newMutableState,
		nil,
		nil,
		targetWorkflowPolicy,
		newWorkflowPolicy.Ptr(),
		nil,
	)
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) executeTransaction(
	ctx context.Context,
	transactionPolicy nDCTransactionPolicy,
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
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
	case nDCTransactionPolicyUpdateAsCurrent:
		return r.updateAsCurrent(
			ctx,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyUpdateAsZombie:
		return r.updateAsZombie(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)

	case nDCTransactionPolicySuppressCurrentAndUpdateAsCurrent:
		return r.suppressCurrentAndUpdateAsCurrent(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsCurrent:
		return r.conflictResolveAsCurrent(
			ctx,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsZombie:
		return r.conflictResolveAsZombie(
			ctx,
			currentWorkflow,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)

	default:
		return serviceerror.NewInternalf("transactionMgr: encountered unknown transaction type: %v", transactionPolicy)
	}
}

func (r *nDCTransactionMgrForExistingWorkflowImpl) cleanupTransaction(
	currentWorkflow Workflow,
	targetWorkflow Workflow,
	newWorkflow Workflow,
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
