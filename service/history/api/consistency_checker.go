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

package api

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	clockspb "go.temporal.io/server/api/clock/v1"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	MutableStateConsistencyPredicate func(mutableState workflow.MutableState) bool

	WorkflowConsistencyChecker interface {
		GetWorkflowCache() wcache.Cache
		GetCurrentRunID(
			ctx context.Context,
			namespaceID string,
			workflowID string,
			lockPriority workflow.LockPriority,
		) (string, error)
		GetWorkflowLease(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			workflowKey definition.WorkflowKey,
			lockPriority workflow.LockPriority,
		) (WorkflowLease, error)

		GetWorkflowLeaseWithConsistancyCheck(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
			lockPriority workflow.LockPriority,
		) (WorkflowLease, error)
	}

	WorkflowConsistencyCheckerImpl struct {
		shardContext  shard.Context
		workflowCache wcache.Cache
	}
)

func NewWorkflowConsistencyChecker(
	shardContext shard.Context,
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
	lockPriority workflow.LockPriority,
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
	lockPriority workflow.LockPriority,
) (WorkflowLease, error) {
	if err := c.clockConsistencyCheck(reqClock); err != nil {
		return nil, err
	}

	if len(workflowKey.RunID) != 0 {
		return c.getWorkflowLease(ctx, workflowKey, lockPriority)
	}

	return c.getCurrentWorkflowContext(
		ctx,
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
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
		// request vector clock cannot is not comparable with current shard vector clock
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

func (c *WorkflowConsistencyCheckerImpl) getWorkflowLease(
	ctx context.Context,
	workflowKey definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (WorkflowLease, error) {
	wfContext, release, err := c.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		c.shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
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

	return NewWorkflowLease(wfContext, release, mutableState), nil
}

func (c *WorkflowConsistencyCheckerImpl) getCurrentWorkflowContext(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	lockPriority workflow.LockPriority,
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
		ctx, definition.NewWorkflowKey(namespaceID, workflowID, runID), lockPriority,
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

// The code below is an old implementation of GetWorkflowLease
// It should be avoided. We should revisit the logic

func (c *WorkflowConsistencyCheckerImpl) GetWorkflowLeaseWithConsistancyCheck(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (WorkflowLease, error) {
	if reqClock != nil {
		currentClock := c.shardContext.CurrentVectorClock()
		if vclock.Comparable(reqClock, currentClock) {
			return c.getWorkflowLeaseValidatedByClock(
				ctx,
				reqClock,
				currentClock,
				workflowKey,
				lockPriority,
			)
		}
		// request vector clock cannot is not comparable with current shard vector clock
		// use methods below
	}

	if len(workflowKey.RunID) != 0 {
		return c.getWorkflowLeaseValidatedByCheck(
			ctx,
			consistencyPredicate,
			workflowKey,
			lockPriority,
		)
	}
	return c.getCurrentWorkflowContextWithConsistencyCheck(
		ctx,
		consistencyPredicate,
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
		lockPriority,
	)
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowLeaseValidatedByCheck(
	ctx context.Context,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (WorkflowLease, error) {
	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"loadWorkflowContext encountered empty run ID: %v", workflowKey,
		))
	}

	wfContext, release, err := c.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		c.shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
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

	if consistencyPredicate(mutableState) {
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

func (c *WorkflowConsistencyCheckerImpl) getCurrentWorkflowContextWithConsistencyCheck(
	ctx context.Context,
	consistencyPredicate MutableStateConsistencyPredicate,
	namespaceID string,
	workflowID string,
	lockPriority workflow.LockPriority,
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
	workflowLease, err := c.getWorkflowLeaseValidatedByCheck(
		ctx,
		consistencyPredicate,
		definition.NewWorkflowKey(namespaceID, workflowID, runID),
		lockPriority,
	)
	if err != nil {
		return nil, err
	}
	if workflowLease.GetMutableState().IsWorkflowExecutionRunning() {
		return workflowLease, nil
	}

	currentRunID, err := c.GetCurrentRunID(
		ctx,
		namespaceID,
		workflowID,
		lockPriority,
	)
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

func (c *WorkflowConsistencyCheckerImpl) getWorkflowLeaseValidatedByClock(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	currentClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
	lockPriority workflow.LockPriority,
) (WorkflowLease, error) {
	cmpResult, err := vclock.Compare(reqClock, currentClock)
	if err != nil {
		return nil, err
	}
	if cmpResult > 0 {
		shardID := c.shardContext.GetShardID()
		c.shardContext.UnloadForOwnershipLost()
		return nil, &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Shard: %v consistency check failed, reloading", shardID),
		}
	}

	wfContext, release, err := c.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		c.shardContext,
		namespace.ID(workflowKey.NamespaceID),
		&commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
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
	return NewWorkflowLease(wfContext, release, mutableState), nil
}
