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
	"bytes"
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

type (
	timerQueueTaskExecutorBase struct {
		currentClusterName string
		shard              shard.Context
		registry           namespace.Registry
		deleteManager      workflow.DeleteManager
		cache              workflow.Cache
		logger             log.Logger
		matchingClient     matchingservice.MatchingServiceClient
		metricProvider     metrics.MetricsHandler
		metricsClient      metrics.Client
		config             *configs.Config
	}
)

func newTimerQueueTaskExecutorBase(
	shard shard.Context,
	workflowCache workflow.Cache,
	deleteManager workflow.DeleteManager,
	matchingClient matchingservice.MatchingServiceClient,
	logger log.Logger,
	metricProvider metrics.MetricsHandler,
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
		metricProvider:     metricProvider,
		metricsClient:      shard.GetMetricsClient(),
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
		workflow.CallerTypeTask,
	)
	if err != nil {
		return err
	}
	defer func() { release(retError) }()

	mutableState, err := loadMutableStateForTimerTask(ctx, weContext, task, t.metricsClient, t.logger)
	switch err.(type) {
	case nil:
		if mutableState == nil {
			return nil
		}
	case *serviceerror.NotFound:
		// the mutable state is deleted and delete history branch operation failed.
		// use task branch token to delete the leftover history branch
		return t.deleteHistoryBranch(task.BranchToken)
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
	if ok := VerifyTaskVersion(t.shard, t.logger, mutableState.GetNamespaceEntry(), lastWriteVersion, task.Version, task); !ok {
		currentBranchToken, err := mutableState.GetCurrentBranchToken()
		if err != nil {
			return err
		}
		// the mutable state has a newer version and the branch token is updated
		// use task branch token to delete the original branch
		if !bytes.Equal(task.BranchToken, currentBranchToken) {
			return t.deleteHistoryBranch(task.BranchToken)
		}
		t.logger.Error("Different mutable state versions have the same branch token", tag.TaskVersion(task.Version), tag.LastEventVersion(lastWriteVersion))
		return serviceerror.NewInternal("Mutable state has different version but same branch token")
	}

	return t.deleteManager.DeleteWorkflowExecutionByRetention(
		ctx,
		namespace.ID(task.GetNamespaceID()),
		workflowExecution,
		weContext,
		mutableState,
	)
}

func getWorkflowExecutionContextForTask(
	ctx context.Context,
	workflowCache workflow.Cache,
	task tasks.Task,
) (workflow.Context, workflow.ReleaseCacheFunc, error) {
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
	workflowCache workflow.Cache,
	namespaceID namespace.ID,
	execution commonpb.WorkflowExecution,
) (workflow.Context, workflow.ReleaseCacheFunc, error) {
	ctx, cancel := context.WithTimeout(ctx, taskGetExecutionTimeout)
	defer cancel()

	weContext, release, err := workflowCache.GetOrCreateWorkflowExecution(
		ctx,
		namespaceID,
		execution,
		workflow.CallerTypeTask,
	)
	if common.IsContextDeadlineExceededErr(err) {
		err = consts.ErrWorkflowBusy
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

func (t *timerQueueTaskExecutorBase) deleteHistoryBranch(branchToken []byte) error {
	if len(branchToken) > 0 {
		return t.shard.GetExecutionManager().DeleteHistoryBranch(context.TODO(), &persistence.DeleteHistoryBranchRequest{
			ShardID:     t.shard.GetShardID(),
			BranchToken: branchToken,
		})
	}
	return nil
}
