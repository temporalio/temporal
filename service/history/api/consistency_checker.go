//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination consistency_checker_mock.go

package api

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/vclock"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	MutableStateConsistencyPredicate func(mutableState historyi.MutableState) bool

	WorkflowConsistencyChecker interface {
		GetWorkflowCache() wcache.Cache
		GetCurrentRunID(
			ctx context.Context,
			namespaceID string,
			workflowID string,
			lockPriority locks.Priority,
		) (string, error)
		GetWorkflowLease(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			workflowKey definition.WorkflowKey,
			lockPriority locks.Priority,
		) (WorkflowLease, error)

		GetWorkflowLeaseWithConsistencyCheck(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
			lockPriority locks.Priority,
		) (WorkflowLease, error)

		GetChasmLease(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			workflowKey definition.WorkflowKey,
			archetype chasm.Archetype,
			lockPriority locks.Priority,
		) (WorkflowLease, error)

		GetChasmLeaseWithConsistencyCheck(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
			archetype chasm.Archetype,
			lockPriority locks.Priority,
		) (WorkflowLease, error)
	}

	WorkflowConsistencyCheckerImpl struct {
		shardContext  historyi.ShardContext
		workflowCache wcache.Cache
	}
)

func NewWorkflowConsistencyChecker(
	shardContext historyi.ShardContext,
	workflowCache wcache.Cache,
) *WorkflowConsistencyCheckerImpl {
	return &WorkflowConsistencyCheckerImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
	}
}

func (c *WorkflowConsistencyCheckerImpl) GetWorkflowCache() wcache.Cache {
	return c.workflowCache
}

func (c *WorkflowConsistencyCheckerImpl) GetCurrentRunID(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	lockPriority locks.Priority,
) (runID string, retErr error) {
	return wcache.GetCurrentRunID(
		ctx,
		c.shardContext,
		c.workflowCache,
		namespaceID,
		workflowID,
		lockPriority,
	)
}

func (c *WorkflowConsistencyCheckerImpl) GetWorkflowLease(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	return c.getWorkflowLeaseImpl(ctx, reqClock, nil, workflowKey, chasmworkflow.Archetype, lockPriority)
}

// The code below should be used when custom workflow state validation is required.
// If consistencyPredicate failed (thus detecting a stale workflow state)
// workflow state will be cleared, and mutable state will be reloaded.
func (c *WorkflowConsistencyCheckerImpl) GetWorkflowLeaseWithConsistencyCheck(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	return c.getWorkflowLeaseImpl(ctx, reqClock, consistencyPredicate, workflowKey, chasmworkflow.Archetype, lockPriority)
}

func (c *WorkflowConsistencyCheckerImpl) GetChasmLease(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	archetype chasm.Archetype,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	return c.getWorkflowLeaseImpl(ctx, reqClock, nil, workflowKey, archetype, lockPriority)
}

func (c *WorkflowConsistencyCheckerImpl) GetChasmLeaseWithConsistencyCheck(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	archetype chasm.Archetype,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	return c.getWorkflowLeaseImpl(ctx, reqClock, consistencyPredicate, workflowKey, archetype, lockPriority)
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowLeaseImpl(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	archetype chasm.Archetype,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	if err := c.clockConsistencyCheck(reqClock); err != nil {
		return nil, err
	}

	if len(workflowKey.RunID) != 0 {
		return c.getWorkflowLease(ctx, consistencyPredicate, workflowKey, archetype, lockPriority)
	}

	return c.getCurrentWorkflowLease(
		ctx,
		consistencyPredicate,
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
		archetype,
		lockPriority,
	)
}

func (c *WorkflowConsistencyCheckerImpl) clockConsistencyCheck(
	reqClock *clockspb.VectorClock,
) error {
	if reqClock == nil {
		return nil
	}
	currentClock := c.shardContext.CurrentVectorClock()
	if !vclock.Comparable(reqClock, currentClock) {
		// request vector clock is not comparable with current shard vector clock
		return nil
	}

	cmpResult, err := vclock.Compare(reqClock, currentClock)
	if err != nil {
		return err
	}
	if cmpResult <= 0 {
		return nil
	}
	shardID := c.shardContext.GetShardID()
	c.shardContext.UnloadForOwnershipLost()
	return &persistence.ShardOwnershipLostError{
		ShardID: shardID,
		Msg:     fmt.Sprintf("Shard: %v consistency check failed, reloading", shardID),
	}
}

func (c *WorkflowConsistencyCheckerImpl) getCurrentWorkflowLease(
	ctx context.Context,
	consistencyPredicate MutableStateConsistencyPredicate,
	namespaceID string,
	workflowID string,
	archetype chasm.Archetype,
	lockPriority locks.Priority,
) (WorkflowLease, error) {
	runID, err := c.GetCurrentRunID(
		ctx,
		namespaceID,
		workflowID,
		lockPriority,
	)
	if err != nil {
		return nil, err
	}
	workflowLease, err := c.getWorkflowLease(
		ctx,
		consistencyPredicate,
		definition.NewWorkflowKey(namespaceID, workflowID, runID),
		archetype,
		lockPriority,
	)

	if err != nil {
		return nil, err
	}
	if workflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		return workflowLease, nil
	}

	currentRunID, err := c.GetCurrentRunID(ctx, namespaceID, workflowID, lockPriority)

	if err != nil {
		workflowLease.GetReleaseFn()(err)
		return nil, err
	}
	if currentRunID == workflowLease.GetContext().GetWorkflowKey().RunID {
		return workflowLease, nil
	}

	workflowLease.GetReleaseFn()(nil)
	return nil, consts.ErrLocateCurrentWorkflowExecution
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowLease(
	ctx context.Context,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	archetype chasm.Archetype,
	lockPriority locks.Priority,
) (WorkflowLease, error) {

	wfContext, release, err := c.workflowCache.GetOrCreateChasmEntity(
		ctx,
		c.shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		archetype,
		lockPriority,
	)
	if err != nil {
		return nil, err
	}

	mutableState, err := wfContext.LoadMutableState(ctx, c.shardContext)
	if err != nil {
		release(err)
		return nil, err
	}

	// if consistencyPredicate is nill we assume it is not needed
	if consistencyPredicate == nil || consistencyPredicate(mutableState) {
		return NewWorkflowLease(wfContext, release, mutableState), nil
	}
	wfContext.Clear()

	mutableState, err = wfContext.LoadMutableState(ctx, c.shardContext)
	if err != nil {
		release(err)
		return nil, err
	}
	return NewWorkflowLease(wfContext, release, mutableState), nil
}
