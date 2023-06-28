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

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

var errUnknownTimerTask = serviceerror.NewInternal("unknown timer task")

type (
	timerQueueTaskExecutorBase struct {
		currentClusterName string
		shard              shard.Context
		registry           namespace.Registry
		deleteManager      deletemanager.DeleteManager
		cache              wcache.Cache
		logger             log.Logger
		matchingClient     matchingservice.MatchingServiceClient
		metricHandler      metrics.Handler
		config             *configs.Config
	}
)

func newTimerQueueTaskExecutorBase(
	shard shard.Context,
	workflowCache wcache.Cache,
	deleteManager deletemanager.DeleteManager,
	matchingClient matchingservice.MatchingServiceClient,
	logger log.Logger,
	metricHandler metrics.Handler,
	config *configs.Config,
) *timerQueueTaskExecutorBase {
	return &timerQueueTaskExecutorBase{
		currentClusterName: shard.GetClusterMetadata().GetCurrentClusterName(),
		shard:              shard,
		registry:           shard.GetNamespaceRegistry(),
		cache:              workflowCache,
		deleteManager:      deleteManager,
		logger:             logger,
		matchingClient:     matchingClient,
		metricHandler:      metricHandler,
		config:             config,
	}
}

func (t *timerQueueTaskExecutorBase) executeDeleteHistoryEventTask(
	ctx context.Context,
	task *tasks.DeleteHistoryEventTask,
) (retError error) {
	ctx, cancel := context.WithTimeout(ctx, taskTimeout)
	defer cancel()

	workflowExecution := commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}

	weContext, release, err := t.cache.GetOrCreateWorkflowExecution(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		workflow.LockPriorityLow,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricHandler, t.logger)
	switch err.(type) {
	case nil:
		if mutableState == nil {
			return nil
		}
	case *serviceerror.NotFound:
		// the mutable state is deleted and delete history branch operation failed.
		// use task branch token to delete the leftover history branch
		return t.deleteHistoryBranch(ctx, task.BranchToken)
	default:
		return err
	}

	if mutableState.GetExecutionState().GetState() != enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED {
		// If workflow is running then just ignore DeleteHistoryEventTask timer task.
		// This should almost never happen because DeleteHistoryEventTask is created only for closed workflows.
		// But cross DC replication can resurrect workflow and therefore DeleteHistoryEventTask should be ignored.
		return nil
	}

	lastWriteVersion, err := mutableState.GetLastWriteVersion()
	if err != nil {
		return err
	}
	if err := CheckTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, task.Version, task); err != nil {
		return err
	}

	// We should only archive if it is enabled, and the data wasn't already archived. If WorkflowDataAlreadyArchived
	// flag is set to true, then the data was already archived, so we can skip it.
	archiveIfEnabled := !task.WorkflowDataAlreadyArchived
	return t.deleteManager.DeleteWorkflowExecutionByRetention(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		weContext,
		mutableState,
		archiveIfEnabled,
		&task.ProcessStage, // Pass stage by reference to update it inside delete manager.
	)
}

func getWorkflowExecutionContextForTask(
	ctx context.Context,
	workflowCache wcache.Cache,
	task tasks.Task,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	namespaceID, execution := getTaskNamespaceIDAndWorkflowExecution(task)
	return getWorkflowExecutionContext(
		ctx,
		workflowCache,
		namespaceID,
		execution,
	)
}

func getWorkflowExecutionContext(
	ctx context.Context,
	workflowCache wcache.Cache,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
) (workflow.Context, wcache.ReleaseCacheFunc, error) {
	// workflowCache will automatically use short context timeout when
	// locking workflow for all background calls, we don't need a separate context here
	weContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.LockPriorityLow,
	)
	if common.IsContextDeadlineExceededErr(err) {
		err = consts.ErrResourceExhaustedBusyWorkflow
	}
	return weContext, release, err
}

func getTaskNamespaceIDAndWorkflowExecution(
	task tasks.Task,
) (namespace.ID, commonpb.WorkflowExecution) {
	return namespace.ID(task.GetNamespaceID()), commonpb.WorkflowExecution{
		WorkflowId: task.GetWorkflowID(),
		RunId:      task.GetRunID(),
	}
}

func (t *timerQueueTaskExecutorBase) deleteHistoryBranch(
	ctx context.Context,
	branchToken []byte,
) error {
	if len(branchToken) > 0 {
		return t.shard.GetExecutionManager().DeleteHistoryBranch(ctx, &persistence.DeleteHistoryBranchRequest{
			ShardID:     t.shard.GetShardID(),
			BranchToken: branchToken,
		})
	}
	return nil
}
