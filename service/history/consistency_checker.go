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

package history

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	clockpb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/vclock"
	"go.temporal.io/server/service/history/workflow"
)

var (
	consistencyCtxKey = consistencyContextKey{}
)

type (
	consistencyContextKey   struct{}
	consistencyContextValue struct {
		checked bool
	}

	MutableStateConsistencyPredicate func(mutableState workflow.MutableState) bool

	WorkflowConsistencyChecker interface {
		GetCurrentRunID(
			ctx context.Context,
			namespaceID string,
			workflowID string,
		) (string, error)
		GetWorkflowContext(
			ctx context.Context,
			reqClock *clockpb.ShardClock,
			consistencyPredicate MutableStateConsistencyPredicate,
			workflowKey definition.WorkflowKey,
		) (workflowContext, error)
	}

	WorkflowConsistencyCheckerImpl struct {
		shardContext  shard.Context
		workflowCache workflow.Cache
	}
)

func newWorkflowConsistencyChecker(
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
	ctx = context.WithValue(ctx, consistencyCtxKey, &consistencyContextValue{})
	runID, err := c.getCurrentRunID(
		ctx,
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
	reqClock *clockpb.ShardClock,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
) (workflowContext, error) {
	if reqClock != nil {
		return c.getWorkflowContextValidatedByClock(
			ctx,
			reqClock,
			workflowKey,
		)
	}

	ctx = context.WithValue(ctx, consistencyCtxKey, &consistencyContextValue{})
	if len(workflowKey.RunID) != 0 {
		return c.getWorkflowContextValidatedByCheckFns(
			ctx,
			consistencyPredicate,
			workflowKey,
		)
	}
	return c.getCurrentWorkflowContext(
		ctx,
		consistencyPredicate,
		workflowKey.NamespaceID,
		workflowKey.WorkflowID,
	)
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowContextValidatedByClock(
	ctx context.Context,
	reqClock *clockpb.ShardClock,
	workflowKey definition.WorkflowKey,
) (workflowContext, error) {
	cmpResult, err := vclock.Compare(reqClock, c.shardContext.CurrentVectorClock())
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
	return newWorkflowContext(wfContext, release, mutableState), nil
}

func (c *WorkflowConsistencyCheckerImpl) getWorkflowContextValidatedByCheckFns(
	ctx context.Context,
	consistencyPredicate MutableStateConsistencyPredicate,
	workflowKey definition.WorkflowKey,
) (workflowContext, error) {
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
			return newWorkflowContext(wfContext, release, mutableState), nil
		}

		wfContext.Clear()
		mutableState, err := wfContext.LoadWorkflowExecution(ctx)
		if err != nil {
			release(err)
			return nil, err
		}
		return newWorkflowContext(wfContext, release, mutableState), nil
	case *serviceerror.NotFound:
		release(err)
		if err := assertShardOwnership(
			ctx,
			c.shardContext,
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
	consistencyPredicate MutableStateConsistencyPredicate,
	namespaceID string,
	workflowID string,
) (workflowContext, error) {
	for attempt := 1; attempt <= conditionalRetryCount; attempt++ {
		runID, err := c.getCurrentRunID(
			ctx,
			namespaceID,
			workflowID,
		)
		if err != nil {
			return nil, err
		}
		wfContext, err := c.getWorkflowContextValidatedByCheckFns(
			ctx,
			consistencyPredicate,
			definition.NewWorkflowKey(namespaceID, workflowID, runID),
		)
		if err != nil {
			return nil, err
		}
		if wfContext.getMutableState().IsWorkflowExecutionRunning() {
			return wfContext, nil
		}

		currentRunID, err := c.getCurrentRunID(
			ctx,
			namespaceID,
			workflowID,
		)
		if err != nil {
			wfContext.getReleaseFn()(err)
			return nil, err
		}
		if currentRunID == wfContext.getRunID() {
			return wfContext, nil
		}
		wfContext.getReleaseFn()(nil)
	}
	return nil, serviceerror.NewUnavailable("unable to locate current workflow execution")
}

func (c *WorkflowConsistencyCheckerImpl) getCurrentRunID(
	ctx context.Context,
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
) error {
	consistencyChecked := ctx.Value(consistencyCtxKey).(*consistencyContextValue)
	if !consistencyChecked.checked {
		consistencyChecked.checked = true
		return shardContext.AssertOwnership(ctx)
	}
	return nil
}

func BypassMutableStateConsistencyPredicate(
	mutableState workflow.MutableState,
) bool {
	return true
}
