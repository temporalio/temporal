//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination transaction_manager_existing_workflow_mock.go

package ndc

import (
	"context"

	"go.temporal.io/api/serviceerror"
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
		// currentRunID == "" means the current execution record is gone — the current run was
		// deleted, leaving this run without a current record. How we re-persist depends on the run:
		//
		//   - closed run with no new run: an orphan that must NOT become current again. Persist via
		//     the bypass-current path (which tolerates a missing current record) so we neither
		//     poison-pill replication nor resurrect a deleted workflow.
		//   - running run, or a run carrying a new run (continue-as-new): a run legitimately must be
		//     current, so reconstruct the current record by inserting a fresh one — pointing at the
		//     new run if carried, otherwise the target run. This aligns the receiver with the active
		//     cluster instead of failing the apply.
		if !mutableState.IsWorkflowExecutionRunning() && newWorkflow == nil {
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
				nil, // no new workflow (guarded above)
				archetypeID,
			)
		}

		if newWorkflow != nil {
			// The reconstruct path below creates the carried new run and points a fresh current record
			// at it. But the new run may already exist in the DB: an earlier delivery (resend/retry)
			// created it, or the new run was independently created by its own state replication, and
			// the current record was later deleted out-of-band (e.g. by delete replication, which drops
			// the current record while a closed run's execution rows survive to retention). Re-creating
			// it would fail the apply with a duplicate-execution error ("Workflow execution already
			// running"), so we must not run brand-new-current with the carried new run.
			newExecutionInfo := newWorkflow.GetMutableState().GetExecutionInfo()
			newExecutionState := newWorkflow.GetMutableState().GetExecutionState()
			newRunID := newExecutionState.RunId
			newWorkflowExists, err := r.transactionMgr.CheckWorkflowExists(
				ctx,
				namespace.ID(newExecutionInfo.NamespaceId),
				newExecutionInfo.WorkflowId,
				newRunID,
				archetypeID,
			)
			if err != nil {
				return err
			}
			if newWorkflowExists {
				// Release the in-memory new run stub; we will not persist it (the DB copy already exists).
				newWorkflow.GetReleaseFn()(nil)

				// Load the already-existing new run to decide whether it should be current. Only a
				// running run is unambiguously the live current run: a running run has no successor and
				// is never a deleted-workflow orphan. If it is running we re-establish the current
				// record pointing at it (otherwise, for an idle continue-as-new successor, no future
				// task would repair the current record - mergeUpdateWithNewReplicationTasks folds the
				// new run's initial replication task into the closing run - and workflow-ID lookups
				// would break permanently). If the existing run is closed, its missing current record
				// means the workflow is being deleted or the run was superseded by a later run; in
				// either case it must NOT become current again, so we leave the current record missing.
				//
				// We re-establish the current record BEFORE applying the target below so the apply is
				// safely retryable: if this call succeeds but the target apply fails, the retry sees a
				// current record again and reaches the normal (non-missing-current) path, which still
				// applies the target. If we applied the target first, a failure after it committed would
				// let the retry dedup the target and skip this repair, leaving the current record gone.
				existingNewWorkflow, err := r.transactionMgr.LoadWorkflow(
					ctx,
					namespaceID,
					workflowID,
					newRunID,
					archetypeID,
				)
				if err != nil {
					return err
				}
				if existingNewWorkflow.GetMutableState().IsWorkflowExecutionRunning() {
					r.shardContext.GetThrottledLogger().Warn(
						"Carried new run already exists and is running while the current execution record is missing; re-establishing the current record for it",
						tag.WorkflowNamespaceID(namespaceID.String()),
						tag.WorkflowID(workflowID),
						tag.WorkflowRunID(targetRunID),
						tag.WorkflowNewRunID(newRunID),
					)
					// The loaded run is not rebuilt, so use the update (not conflict-resolve) path.
					if err := r.dispatchWorkflowUpdateAsCurrentBrandNew(
						ctx,
						false, // the loaded run is not rebuilt
						existingNewWorkflow,
						nil, // no new run carried: we only re-establish the current record
						archetypeID,
					); err != nil {
						return err
					}
				} else {
					// closed run: do not resurrect it as current.
					existingNewWorkflow.GetReleaseFn()(nil)
				}

				// Apply the (closed) target via bypass-current. The new run already exists so it is not
				// re-created, and bypass-current tolerates the missing/other current record.
				return r.dispatchWorkflowUpdateAsZombie(
					ctx,
					isWorkflowRebuilt,
					nil, // no current workflow to suppress against
					targetWorkflow,
					nil, // new run already persisted; do not re-create it
					archetypeID,
				)
			}
		}

		r.shardContext.GetThrottledLogger().Warn(
			"Reconstructing missing current execution record on replication apply",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(targetRunID),
		)
		return r.dispatchWorkflowUpdateAsCurrentBrandNew(
			ctx,
			isWorkflowRebuilt,
			targetWorkflow,
			newWorkflow,
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

func (r *nDCTransactionMgrForExistingWorkflowImpl) dispatchWorkflowUpdateAsCurrentBrandNew(
	ctx context.Context,
	isWorkflowRebuilt bool,
	targetWorkflow Workflow,
	newWorkflow Workflow,
	archetypeID chasm.ArchetypeID,
) error {

	if !isWorkflowRebuilt {
		return r.executeTransaction(
			ctx,
			nDCTransactionPolicyUpdateAsCurrentBrandNew,
			nil,
			targetWorkflow,
			newWorkflow,
			archetypeID,
		)
	}

	return r.executeTransaction(
		ctx,
		nDCTransactionPolicyConflictResolveAsCurrentBrandNew,
		nil,
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

// updateAsCurrentBrandNew updates the target run and inserts a brand-new current execution record
// (failing if one already exists), pointing it at the new run if one is carried, otherwise the
// target. It is the reconstruct path for a missing current record (currentRunID == "") on the
// non-rebuilt apply. No suppression happens: the run being pointed at is meant to be current.
func (r *nDCTransactionMgrForExistingWorkflowImpl) updateAsCurrentBrandNew(
	ctx context.Context,
	targetWorkflow Workflow,
	newWorkflow Workflow,
) error {

	var newContext historyi.WorkflowContext
	var newMutableState historyi.MutableState
	var newTransactionPolicy *historyi.TransactionPolicy
	if newWorkflow != nil {
		newContext = newWorkflow.GetContext()
		newMutableState = newWorkflow.GetMutableState()
		newTransactionPolicy = historyi.TransactionPolicyPassive.Ptr()
	}

	return targetWorkflow.GetContext().UpdateWorkflowExecutionWithNew(
		ctx,
		r.shardContext,
		persistence.UpdateWorkflowModeBrandNewCurrent,
		newContext,
		newMutableState,
		historyi.TransactionPolicyPassive,
		newTransactionPolicy,
	)
}

// suppressTargetPolicy suppresses the target workflow by the current workflow and returns the
// resulting transaction policy.
//
// It exists solely for the deleted-current-run (orphan) path in this file, where currentWorkflow is
// nil because the current execution record is gone. In that case there is nothing to suppress
// against and the target is guaranteed closed (dispatchForExistingWorkflow only reaches the zombie
// path for a non-running target), so it stays passive. Do NOT reuse this elsewhere or call it with a
// nil current for a running target: it would report the target as suppressed without zombifying it.
// Everywhere else, call targetWorkflow.SuppressBy(currentWorkflow) directly.
func suppressTargetPolicy(targetWorkflow Workflow, currentWorkflow Workflow) (historyi.TransactionPolicy, error) {
	if currentWorkflow == nil {
		return historyi.TransactionPolicyPassive, nil
	}
	return targetWorkflow.SuppressBy(currentWorkflow)
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
		// newWorkflow is only set when a current workflow exists (dispatchForExistingWorkflow skips
		// the new run when the current record is missing), so suppressing against it is safe.
		newWorkflowPolicy, err := newWorkflow.SuppressBy(
			currentWorkflow,
		)
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

// conflictResolveAsCurrentBrandNew is the rebuilt/snapshot counterpart of updateAsCurrentBrandNew:
// it conflict-resolves the target run and inserts a brand-new current execution record (failing if
// one already exists), pointing it at the new run if carried, otherwise the target. Used to
// reconstruct a missing current record (currentRunID == "") when the workflow was rebuilt.
func (r *nDCTransactionMgrForExistingWorkflowImpl) conflictResolveAsCurrentBrandNew(
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
		persistence.ConflictResolveWorkflowModeBrandNewCurrent,
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
		// newWorkflow is only set when a current workflow exists (dispatchForExistingWorkflow skips
		// the new run when the current record is missing), so suppressing against it is safe.
		newWorkflowPolicy, err = newWorkflow.SuppressBy(
			currentWorkflow,
		)
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

	case nDCTransactionPolicyUpdateAsCurrentBrandNew:
		return r.updateAsCurrentBrandNew(
			ctx,
			targetWorkflow,
			newWorkflow,
		)

	case nDCTransactionPolicyConflictResolveAsCurrentBrandNew:
		return r.conflictResolveAsCurrentBrandNew(
			ctx,
			targetWorkflow,
			newWorkflow,
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
