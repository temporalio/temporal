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

	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

// CallbackAddressingTask is a task that addresses a workflow callback.
type CallbackAddressingTask interface {
	tasks.Task
	GetCallbackID() string
	GetTransitionCount() int32
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
		namespace.ID(task.GetNamespaceID()),
		&commonpb.WorkflowExecution{
			WorkflowId: task.GetWorkflowID(),
			RunId:      task.GetRunID(),
		},
	)
}

func getWorkflowExecutionContext(
	ctx context.Context,
	shardContext shard.Context,
	workflowCache wcache.Cache,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
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
// Does **not** reload mutable state when it validate reports it is stale. Not meant to be called directly, call
// [loadAndValidateMutableState] instead.
func (l *taskExecutor) loadAndValidateMutableStateNoReload(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState) error,
) (workflow.MutableState, error) {
	mutableState, err := wfCtx.LoadMutableState(ctx, l.shardContext)
	if err != nil {
		return nil, err
	}

	return mutableState, validate(mutableState)
}

// loadAndValidateMutableState loads mutable state and validates it.
// Propagages errors returned from validate.
// Reloads mutable state and retries if validator returns a [queues.StaleStateError].
func (l *taskExecutor) loadAndValidateMutableState(
	ctx context.Context,
	wfCtx workflow.Context,
	validate func(workflow.MutableState) error,
) (workflow.MutableState, error) {
	mutableState, err := l.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate)
	if err == nil {
		return mutableState, nil
	}

	var sve queues.StaleStateError
	if !errors.As(err, &sve) {
		return nil, err
	}
	l.metricsHandler.Counter(metrics.StaleMutableStateCounter.Name()).Record(1)
	wfCtx.Clear()

	return l.loadAndValidateMutableStateNoReload(ctx, wfCtx, validate)
}

// validateTaskVersion validates task version against the given stateVersion.
// If namespace is not a global namepace, this always returns nil.
// The returned error is always a [queues.TaskStateMismatchError].
func (l *taskExecutor) validateTaskVersion(
	ns *namespace.Namespace,
	stateVersion int64,
	task tasks.Task,
) error {
	if !l.shardContext.GetClusterMetadata().IsGlobalNamespaceEnabled() {
		return nil
	}

	// the first return value is whether this task is valid for further processing
	if !ns.IsGlobalNamespace() {
		l.logger.Debug("NamespaceID is not global, task version check pass", tag.WorkflowNamespaceID(ns.ID().String()), tag.Task(task))
		return nil
	}
	if stateVersion < task.GetVersion() {
		return queues.NewStateStaleError("state version < task version")
	}
	if stateVersion > task.GetVersion() {
		return fmt.Errorf("%w: state version > task version", queues.ErrStaleTask)
	}
	l.logger.Debug("NamespaceID is global, task version == target version", tag.WorkflowNamespaceID(ns.ID().String()), tag.Task(task), tag.TaskVersion(task.GetVersion()))
	return nil
}

// validateCallbackTask compares the task and associated callback's version and transition count to detect staleness.
func (l *taskExecutor) validateCallbackTask(ms workflow.MutableState, task CallbackAddressingTask) (*persistencespb.CallbackInfo, error) {
	ns := ms.GetNamespaceEntry()
	callbackID := task.GetCallbackID()
	callback, ok := ms.GetExecutionInfo().GetCallbacks()[callbackID]
	if !ok {
		// Some unexpected process deleted the callback from mutable state or mutable state is stale.
		return nil, queues.NewStateStaleError(fmt.Sprintf("invalid callback ID for task: %s", callbackID))
	}

	if err := l.validateTaskVersion(ns, callback.GetNamespaceFailoverVersion(), task); err != nil {
		return nil, err
	}

	if callback.TransitionCount < task.GetTransitionCount() {
		return nil, queues.NewStateStaleError("callback transitions < task transitions")
	}
	if callback.TransitionCount > task.GetTransitionCount() {
		return nil, fmt.Errorf("%w: callback transitions > task transitions", queues.ErrStaleTask)
	}
	return callback, nil
}

// getValidatedMutableStateForTask loads mutable state and validates it with the given function.
// validate must not mutate the state.
func (l *taskExecutor) getValidatedMutableStateForTask(
	ctx context.Context,
	task tasks.Task,
	validate func(workflow.MutableState) error,
) (workflow.Context, wcache.ReleaseCacheFunc, workflow.MutableState, error) {
	wfCtx, release, err := getWorkflowExecutionContextForTask(ctx, l.shardContext, l.cache, task)
	if err != nil {
		return nil, nil, nil, err
	}

	ms, err := l.loadAndValidateMutableState(ctx, wfCtx, validate)

	if err != nil {
		// Release now with no error to prevent mutable state from being unloaded from the cache.
		release(nil)
		return nil, nil, nil, err
	}
	return wfCtx, release, ms, nil
}
