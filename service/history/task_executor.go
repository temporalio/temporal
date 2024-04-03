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
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"google.golang.org/protobuf/proto"
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
	return getWorkflowExecutionContext(ctx, shardContext, workflowCache, taskWorkflowKey(task))
}

func getWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	key definition.WorkflowKey,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
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
		workflow.LockPriorityLow,
	)
	if common.IsContextDeadlineExceededErr(err) {
		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	return weContext, release, err
}

// taskExecutor provides basic functionality for task execution.
type taskExecutor struct {
	shardContext   shard.Context
	cache          wcache.Cache
	metricsHandler metrics.Handler
	logger         log.Logger
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Does **not** reload mutable state if validate reports it is stale. Not meant to be called directly, call
// [loadAndValidateMutableState] instead.
func (t *taskExecutor) loadAndValidateMutableStateNoReload(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState) error,
) (workflow.MutableState, error) {
	mutableState, err := wfCtx.LoadMutableState(ctx, t.shardContext)
	if err != nil {
		return nil, err
	}

	return mutableState, validate(mutableState)
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Reloads mutable state and retries if validator returns a [queues.StaleStateError].
func (t *taskExecutor) loadAndValidateMutableState(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState) error,
) (workflow.MutableState, error) {
	mutableState, err := t.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate)
	if err == nil {
		return mutableState, nil
	}

	var sve queues.StaleStateError
	if !errors.As(err, &sve) {
		return nil, err
	}
	t.metricsHandler.Counter(metrics.StaleMutableStateCounter.Name()).Record(1)
	wfCtx.Clear()

	return t.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate)
}

// validateStateMachineTask compares the task and associated state machine's version and transition count to detect staleness.
func (t *taskExecutor) validateStateMachineTask(ms workflow.MutableState, ref hsm.Ref) error {
	err := workflow.TransitionHistoryStalenessCheck(
		ms.GetExecutionInfo().GetTransitionHistory(),
		ref.StateMachineRef.MutableStateNamespaceFailoverVersion,
		ref.StateMachineRef.MutableStateTransitionCount,
	)
	if err != nil {
		return err
	}
	node, err := ms.HSM().Child(ref.StateMachinePath())
	if err != nil {
		return fmt.Errorf("%w: %w", queues.NewUnprocessableTaskError("node lookup failed"), err)
	}

	// Only check for strict equality if the ref has non zero MachineTransitionCount, which marks the task as non-concurrent.
	if ref.StateMachineRef.MachineTransitionCount != 0 && node.TransitionCount() != ref.StateMachineRef.MachineTransitionCount {
		return fmt.Errorf("%w: state machine transitions != task transitions", queues.ErrStaleTask)
	}
	return nil
}

// getValidatedMutableState loads mutable state and validates it with the given function.
// validate must not mutate the state.
func (t *taskExecutor) getValidatedMutableState(
	ctx context.Context,
	key definition.WorkflowKey,
	validate func(workflow.MutableState) error,
) (workflow.Context, wcache.ReleaseCacheFunc, workflow.MutableState, error) {
	wfCtx, release, err := getWorkflowExecutionContext(ctx, t.shardContext, t.cache, key)
	if err != nil {
		return nil, nil, nil, err
	}

	ms, err := t.loadAndValidateMutableState(ctx, wfCtx, validate)

	if err != nil {
		// Release now with no error to prevent mutable state from being unloaded from the cache.
		release(nil)
		return nil, nil, nil, err
	}
	return wfCtx, release, ms, nil
}

func (t *taskExecutor) LoadHistoryEvent(ctx context.Context, def definition.WorkflowKey, token []byte) (*historypb.HistoryEvent, error) {
	ref := &tokenspb.HistoryEventRef{}
	err := proto.Unmarshal(token, ref)
	if err != nil {
		return nil, err
	}
	eventKey := events.EventKey{
		NamespaceID: namespace.ID(def.NamespaceID),
		WorkflowID:  def.WorkflowID,
		RunID:       def.RunID,
		EventID:     ref.EventId,
		Version:     ref.NamespaceFailoverVersion,
	}
	return t.shardContext.GetEventsCache().GetEvent(ctx, t.shardContext.GetShardID(), eventKey, ref.EventBatchId, ref.BranchToken)
}

func (t *taskExecutor) Access(ctx context.Context, ref hsm.Ref, accessType hsm.AccessType, accessor func(*hsm.Node) error) (retErr error) {
	wfCtx, release, ms, err := t.getValidatedMutableState(
		ctx, ref.WorkflowKey, func(ms workflow.MutableState) error {
			return t.validateStateMachineTask(ms, ref)
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
		return wfCtx.SubmitClosedWorkflowSnapshot(ctx, t.shardContext, workflow.TransactionPolicyActive)
	}
	return wfCtx.UpdateWorkflowExecutionAsActive(ctx, t.shardContext)
}

func (t *taskExecutor) Now() time.Time {
	return t.shardContext.GetTimeSource().Now()
}
