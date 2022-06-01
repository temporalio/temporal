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
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

type (
	MutableStateConsistencyPredicate func(mutableState workflow.MutableState) bool

	WorkflowConsistencyChecker interface {
		GetCurrentRunID(
			ctx context.Context,
			namespaceID string,
			workflowID string,
		) (string, error)
		GetWorkflowContext(
			ctx context.Context,
			reqClock *clockspb.VectorClock,
			consistencyPredicate MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
		) (WorkflowContext, error)
	}

	WorkflowConsistencyCheckerImpl struct {
		shardContext  shard.Context
		workflowCache workflow.Cache
	}
)

func NewWorkflowConsistencyChecker(
	shardContext shard.Context,
	workflowCache workflow.Cache,
) *WorkflowConsistencyCheckerImpl {
	return &WorkflowConsistencyCheckerImpl{
		shardContext:  shardContext,
		workflowCache: workflowCache,
	}
}

func (c *WorkflowConsistencyCheckerImpl) GetCurrentRunID(
	ctx context.Context,
	namespaceID string,
	workflowID string,
) (string, error) {
	// to achieve read after write consistency,
	// logic need to assert shard ownership *at most once* per read API call
	// shard ownership asserted boolean keep tracks whether AssertOwnership is already caller
	shardOwnershipAsserted := false
	runID, err := c.getCurrentRunID(
		ctx,
		&shardOwnershipAsserted,
		namespaceID,
		workflowID,
	)
	if err != nil {
		return "", err
	}
	return runID, nil
}

func (c *WorkflowConsistencyCheckerImpl) GetWorkflowContext(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
) (WorkflowContext, error) {
	if reqClock != nil {
		currentClock := c.shardContext.CurrentVectorClock()
		if vclock.Comparable(reqClock, currentClock) {
			return c.getWorkflowContextValidatedByClock(
				ctx,
				reqClock,
				currentClock,
				workflowKey,
			)
		}
		// request vector clock cannot is not comparable with current shard vector clock
		// use methods below
	}

	// to achieve read after write consistency,
	// logic need to assert shard ownership *at most once* per read API call
	// shard ownership asserted boolean keep tracks whether AssertOwnership is already caller
	shardOwnershipAsserted := false
	if len(workflowKey.RunID) != 0 {
		return c.getWorkflowContextValidatedByCheck(
			ctx,
			&shardOwnershipAsserted,
			consistencyPredicate,
			workflowKey,
		)
	}
	return c.getCurrentWorkflowContext(
		ctx,
		&shardOwnershipAsserted,
		consistencyPredicate,
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
	)
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowContextValidatedByClock(
	ctx context.Context,
	reqClock *clockspb.VectorClock,
	currentClock *clockspb.VectorClock,
	workflowKey definition.WorkflowKey,
) (WorkflowContext, error) {
	cmpResult, err := vclock.Compare(reqClock, currentClock)
	if err != nil {
		return nil, err
	}
	if cmpResult > 0 {
		shardID := c.shardContext.GetShardID()
		c.shardContext.Unload()
		return nil, &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg:     fmt.Sprintf("Shard: %v consistency check failed, reloading", shardID),
		}
	}

	wfContext, release, err := c.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}

	mutableState, err := wfContext.LoadWorkflowExecution(ctx)
	if err != nil {
		release(err)
		return nil, err
	}
	return NewWorkflowContext(wfContext, release, mutableState), nil
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowContextValidatedByCheck(
	ctx context.Context,
	shardOwnershipAsserted *bool,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
) (WorkflowContext, error) {
	if len(workflowKey.RunID) == 0 {
		return nil, serviceerror.NewInternal(fmt.Sprintf(
			"loadWorkflowContext encountered empty run ID: %v", workflowKey,
		))
	}

	wfContext, release, err := c.workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(workflowKey.NamespaceID),
		commonpb.WorkflowExecution{
			WorkflowId: workflowKey.WorkflowID,
			RunId:      workflowKey.RunID,
		},
		workflow.CallerTypeAPI,
	)
	if err != nil {
		return nil, err
	}

	mutableState, err := wfContext.LoadWorkflowExecution(ctx)
	switch err.(type) {
	case nil:
		if consistencyPredicate(mutableState) {
			return NewWorkflowContext(wfContext, release, mutableState), nil
		}
		wfContext.Clear()

		mutableState, err := wfContext.LoadWorkflowExecution(ctx)
		if err != nil {
			release(err)
			return nil, err
		}
		return NewWorkflowContext(wfContext, release, mutableState), nil
	case *serviceerror.NotFound, *serviceerror.NamespaceNotFound:
		release(err)
		if err := assertShardOwnership(
			ctx,
			c.shardContext,
			shardOwnershipAsserted,
		); err != nil {
			return nil, err
		}
		return nil, err
	default:
		release(err)
		return nil, err
	}
}

func (c *WorkflowConsistencyCheckerImpl) getCurrentWorkflowContext(
	ctx context.Context,
	shardOwnershipAsserted *bool,
	consistencyPredicate MutableStateConsistencyPredicate,
	namespaceID string,
	workflowID string,
) (WorkflowContext, error) {
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		runID, err := c.getCurrentRunID(
			ctx,
			shardOwnershipAsserted,
			namespaceID,
			workflowID,
		)
		if err != nil {
			return nil, err
		}
		wfContext, err := c.getWorkflowContextValidatedByCheck(
			ctx,
			shardOwnershipAsserted,
			consistencyPredicate,
			definition.NewWorkflowKey(namespaceID, workflowID, runID),
		)
		if err != nil {
			return nil, err
		}
		if wfContext.GetMutableState().IsWorkflowExecutionRunning() {
			return wfContext, nil
		}

		currentRunID, err := c.getCurrentRunID(
			ctx,
			shardOwnershipAsserted,
			namespaceID,
			workflowID,
		)
		if err != nil {
			wfContext.GetReleaseFn()(err)
			return nil, err
		}
		if currentRunID == wfContext.GetRunID() {
			return wfContext, nil
		}
		wfContext.GetReleaseFn()(nil)
	}
	return nil, serviceerror.NewUnavailable("unable to locate current workflow execution")
}

func (c *WorkflowConsistencyCheckerImpl) getCurrentRunID(
	ctx context.Context,
	shardOwnershipAsserted *bool,
	namespaceID string,
	workflowID string,
) (string, error) {
	resp, err := c.shardContext.GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     c.shardContext.GetShardID(),
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
		},
	)
	switch err.(type) {
	case nil:
		return resp.RunID, nil
	case *serviceerror.NotFound:
		if err := assertShardOwnership(
			ctx,
			c.shardContext,
			shardOwnershipAsserted,
		); err != nil {
			return "", err
		}
		return "", err
	default:
		return "", err
	}
}

func assertShardOwnership(
	ctx context.Context,
	shardContext shard.Context,
	shardOwnershipAsserted *bool,
) error {
	if !*shardOwnershipAsserted {
		*shardOwnershipAsserted = true
		return shardContext.AssertOwnership(ctx)
	}
	return nil
}

func BypassMutableStateConsistencyPredicate(
	mutableState workflow.MutableState,
) bool {
	return true
}

func FailMutableStateConsistencyPredicate(
	mutableState workflow.MutableState,
) bool {
	return false
}

func HistoryEventConsistencyPredicate(
	eventID int64,
	eventVersion int64,
) MutableStateConsistencyPredicate {
	return func(mutableState workflow.MutableState) bool {
		if eventVersion != 0 {
			_, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(
				mutableState.GetExecutionInfo().GetVersionHistories(),
				versionhistory.NewVersionHistoryItem(eventID, eventVersion),
			)
			return err == nil
		}
		// if initiated version is 0, it means the namespace is local or
		// the caller has old version and does not sent the info.
		// in either case, fallback to comparing next eventID
		return eventID < mutableState.GetNextEventID()
	}
}
