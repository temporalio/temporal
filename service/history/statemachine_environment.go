// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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
	"context"
	"errors"
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

func taskWorkflowKey(task tasks.Task) definition.WorkflowKey {
	return definition.NewWorkflowKey(task.GetNamespaceID(), task.GetWorkflowID(), task.GetRunID())
}

func getWorkflowExecutionContextForTask(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	task tasks.Task,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	return getWorkflowExecutionContext(
		ctx,
		shardContext,
		workflowCache,
		taskWorkflowKey(task),
		workflow.LockPriorityLow,
	)
}

func getWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	key definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	if key.GetRunID() == "" {
		return getCurrentWorkflowExecutionContext(
			ctx,
			shardContext,
			workflowCache,
			key.NamespaceID,
			key.WorkflowID,
			lockPriority,
		)
	}

	namespaceID := namespace.ID(key.GetNamespaceID())
	execution := &commonpb.WorkflowExecution{
		WorkflowId: key.GetWorkflowID(),
		RunId:      key.GetRunID(),
	}
	// workflowCache will automatically use short context timeout when
	// locking workflow for all background calls, we don't need a separate context here
	weContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		shardContext,
		namespaceID,
		execution,
		lockPriority,
	)
	if common.IsContextDeadlineExceededErr(err) {
		// TODO: make sure this doesn't count against our SLA if this happens while handling an API request.
		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	return weContext, release, err
}

func getCurrentWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	namespaceID string,
	workflowID string,
	lockPriority workflow.LockPriority,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	currentRunID, err := wcache.GetCurrentRunID(
		ctx,
		shardContext,
		workflowCache,
		namespaceID,
		workflowID,
		lockPriority,
	)
	if err != nil {
		return nil, nil, err
	}

	wfContext, release, err := getWorkflowExecutionContext(
		ctx,
		shardContext,
		workflowCache,
		definition.NewWorkflowKey(namespaceID, workflowID, currentRunID),
		lockPriority,
	)
	if err != nil {
		return nil, nil, err
	}

	mutableState, err := wfContext.LoadMutableState(ctx, shardContext)
	if err != nil {
		release(err)
		return nil, nil, err
	}

	if mutableState.IsWorkflowExecutionRunning() {
		return wfContext, release, nil
	}

	// for close workflow we need to check if it is still the current run
	// since it's possible that the workflowID has a newer run before it's locked

	currentRunID, err = wcache.GetCurrentRunID(
		ctx,
		shardContext,
		workflowCache,
		namespaceID,
		workflowID,
		lockPriority,
	)
	if err != nil {
		// release with nil error to prevent mutable state from being unloaded from the cache
		release(nil)
		return nil, nil, err
	}

	if currentRunID != wfContext.GetWorkflowKey().RunID {
		// release with nil error to prevent mutable state from being unloaded from the cache
		release(nil)
		return nil, nil, consts.ErrLocateCurrentWorkflowExecution
	}

	return wfContext, release, nil
}

// stateMachineEnvironment provides basic functionality for state machine task execution and handling of API requests.
type stateMachineEnvironment struct {
	shardContext   shard.Context
	cache          wcache.Cache
	metricsHandler metrics.Handler
	logger         log.Logger
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Does **not** reload mutable state if validate reports it is stale. Not meant to be called directly, call
// [loadAndValidateMutableState] instead.
func (e *stateMachineEnvironment) loadAndValidateMutableStateNoReload(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(ms workflow.MutableState, potentialStaleState bool) error,
	potentialStaleState bool,
) (workflow.MutableState, error) {
	mutableState, err := wfCtx.LoadMutableState(ctx, e.shardContext)
	if err != nil {
		return nil, err
	}

	return mutableState, validate(mutableState, potentialStaleState)
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Reloads mutable state and retries if validator returns a [queues.StaleStateError].
func (e *stateMachineEnvironment) loadAndValidateMutableState(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(ms workflow.MutableState, potentialStaleState bool) error,
) (workflow.MutableState, error) {
	mutableState, err := e.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate, true)
	if err == nil {
		return mutableState, nil
	}

	if !errors.Is(err, consts.ErrStaleState) {
		return nil, err
	}
	e.metricsHandler.Counter(metrics.StaleMutableStateCounter.Name()).Record(1)
	wfCtx.Clear()

	return e.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate, false)
}

// validateStateMachineRef compares the ref and associated state machine's version and transition count to detect staleness.
func (e *stateMachineEnvironment) validateStateMachineRef(ms workflow.MutableState, ref hsm.Ref, potentialStaleState bool) error {
	if err := validateTaskGeneration(ms, ref.TaskID); err != nil {
		return err
	}

	if ref.StateMachineRef.MutableStateVersionedTransition == nil ||
		ref.StateMachineRef.MachineInitialVersionedTransition.TransitionCount == 0 ||
		(ref.StateMachineRef.MachineLastUpdateVersionedTransition != nil &&
			ref.StateMachineRef.MachineLastUpdateVersionedTransition.TransitionCount == 0) {
		// Transtion history was disabled when the ref is generated,
		// fallback to the old validation logic.
		return e.validateStateMachineRefWithoutTransitionHistory(ms, ref, potentialStaleState)
	}

	err := workflow.TransitionHistoryStalenessCheck(
		ms.GetExecutionInfo().GetTransitionHistory(),
		ref.StateMachineRef.MutableStateVersionedTransition,
	)
	if err != nil {
		return err
	}
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			return fmt.Errorf("%w: %w", consts.ErrStaleReference, err)
		}
		return fmt.Errorf("%w: %w", serviceerror.NewInternal("node lookup failed"), err)
	}

	if node.InternalRepr().GetInitialVersionedTransition().TransitionCount == 0 {
		// transition history was disabled after the ref was generated and mutable state got rebuilt.
		// fallback to the old validation logic.
		return e.validateStateMachineRefWithoutTransitionHistory(ms, ref, potentialStaleState)
	}

	if workflow.CompareVersionedTransition(
		ref.StateMachineRef.MachineInitialVersionedTransition,
		node.InternalRepr().GetInitialVersionedTransition(),
	) != 0 {
		return fmt.Errorf("%w: initial versioned transition mismatch", consts.ErrStaleReference)
	}

	if ref.StateMachineRef.MachineLastUpdateVersionedTransition == nil {
		// ref is concurrent or when transition history was disabled when the node was last updated.
		return nil
	}

	if node.InternalRepr().GetLastUpdateVersionedTransition().TransitionCount == 0 {
		// transition history was disabled after the ref was generated.
		// fallback to the old validation logic.
		return e.validateStateMachineRefWithoutTransitionHistory(ms, ref, potentialStaleState)
	}

	// Non-nil MachineLastUpdatedVersionedTransition marks the reference as non-concurrent and can be
	// invalidated with the check below.
	if workflow.CompareVersionedTransition(
		ref.StateMachineRef.MachineLastUpdateVersionedTransition,
		node.InternalRepr().GetLastUpdateVersionedTransition(),
	) != 0 {
		return fmt.Errorf("%w: last update versioned transition mismatch", consts.ErrStaleReference)
	}

	return nil
}

func (e *stateMachineEnvironment) validateStateMachineRefWithoutTransitionHistory(ms workflow.MutableState, ref hsm.Ref, potentialStaleState bool) error {
	// Ignore potentialStaleState if the reference cannot reference stale state (e.g if it came from task executor and
	// not an API request).
	potentialStaleState = potentialStaleState && ref.TaskID == 0

	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		if errors.Is(err, hsm.ErrStateMachineNotFound) {
			if potentialStaleState {
				return fmt.Errorf("%w: %w", consts.ErrStaleState, err)
			}
			// We checked above that mutable state is up-to-date with our ref. If we can't find the state machine node,
			// we must assume the reference is stale.
			// This isn't bulletproof since the ref may have been generated on a different cluster and come from an API
			// request before the state has been replicated to the current cluster.
			// We accept the imperfection here and plan to solve it with the introduction of transition history.
			return fmt.Errorf("%w: %w", consts.ErrStaleReference, err)
		}
		return fmt.Errorf("%w: %w", serviceerror.NewInternal("node lookup failed"), err)
	}

	if node.InternalRepr().InitialVersionedTransition.NamespaceFailoverVersion !=
		ref.StateMachineRef.MachineInitialVersionedTransition.NamespaceFailoverVersion {
		if potentialStaleState {
			return fmt.Errorf("%w: state machine ref initial failover version mismatch", consts.ErrStaleState)
		}
		return fmt.Errorf("%w: state machine ref initial failover version mismatch", consts.ErrStaleReference)
	}

	// Only check for strict equality if the ref has non zero MachineTransitionCount, which marks the task as non-concurrent.
	if ref.StateMachineRef.MachineTransitionCount != 0 &&
		node.InternalRepr().GetTransitionCount() != ref.StateMachineRef.MachineTransitionCount {
		return fmt.Errorf("%w: state machine transitions != ref transitions", consts.ErrStaleReference)
	}

	return nil
}

// getValidatedMutableState loads mutable state and validates it with the given function.
// validate must not mutate the state.
func (e *stateMachineEnvironment) getValidatedMutableState(
	ctx context.Context,
	key definition.WorkflowKey,
	validate func(ms workflow.MutableState, potentialStaleState bool) error,
) (workflow.Context, wcache.ReleaseCacheFunc, workflow.MutableState, error) {
	wfCtx, release, err := getWorkflowExecutionContext(ctx, e.shardContext, e.cache, key, workflow.LockPriorityLow)
	if err != nil {
		return nil, nil, nil, err
	}

	ms, err := e.loadAndValidateMutableState(ctx, wfCtx, validate)

	if err != nil {
		// Release now with no error to prevent mutable state from being unloaded from the cache.
		release(nil)
		return nil, nil, nil, err
	}
	return wfCtx, release, ms, nil
}

func (e *stateMachineEnvironment) validateNotZombieWorkflow(
	ms workflow.MutableState,
	accessType hsm.AccessType,
) error {
	// need to specifically check for zombie workflows here instead of workflow running
	// or not since zombie workflows are considered as not running but closed workflow
	// can still be updated
	if accessType == hsm.AccessWrite &&
		ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE {
		return consts.ErrWorkflowZombie
	}
	return nil
}

func (e *stateMachineEnvironment) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) (retErr error) {
	wfCtx, release, ms, err := e.getValidatedMutableState(
		ctx, ref.WorkflowKey, func(ms workflow.MutableState, potentialStaleState bool) error {
			if err := e.validateNotZombieWorkflow(ms, accessType); err != nil {
				return err
			}
			return e.validateStateMachineRef(ms, ref, potentialStaleState)
		},
	)
	if err != nil {
		return err
	}
	var accessed bool
	defer func() {
		if accessType == hsm.AccessWrite && accessed {
			release(retErr)
		} else {
			release(nil)
		}
	}()
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		return err
	}
	accessed = true
	if err := accessor(node); err != nil {
		return err
	}
	if accessType == hsm.AccessRead {
		return nil
	}

	if ms.GetExecutionState().State == enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// Can't use UpdateWorkflowExecutionAsActive since it updates the current run, and we are operating on closed
		// workflows.
		return wfCtx.SubmitClosedWorkflowSnapshot(ctx, e.shardContext, workflow.TransactionPolicyActive)
	}
	return wfCtx.UpdateWorkflowExecutionAsActive(ctx, e.shardContext)
}

func (e *stateMachineEnvironment) Now() time.Time {
	return e.shardContext.GetTimeSource().Now()
}
