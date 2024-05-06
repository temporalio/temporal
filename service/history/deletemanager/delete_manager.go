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

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination delete_manager_mock.go

package deletemanager

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	DeleteManager interface {
		AddDeleteWorkflowExecutionTask(
			ctx context.Context,
			nsID namespace.ID,
			we *commonpb.WorkflowExecution,
			ms workflow.MutableState,
		) error
		DeleteWorkflowExecution(
			ctx context.Context,
			nsID namespace.ID,
			we *commonpb.WorkflowExecution,
			weCtx workflow.Context,
			ms workflow.MutableState,
			forceDeleteFromOpenVisibility bool,
			stage *tasks.DeleteWorkflowExecutionStage,
		) error
		DeleteWorkflowExecutionByRetention(
			ctx context.Context,
			nsID namespace.ID,
			we *commonpb.WorkflowExecution,
			weCtx workflow.Context,
			ms workflow.MutableState,
			stage *tasks.DeleteWorkflowExecutionStage,
		) error
	}

	DeleteManagerImpl struct {
		shardContext      shard.Context
		workflowCache     wcache.Cache
		config            *configs.Config
		metricsHandler    metrics.Handler
		timeSource        clock.TimeSource
		visibilityManager manager.VisibilityManager
	}
)

var _ DeleteManager = (*DeleteManagerImpl)(nil)

func NewDeleteManager(
	shardContext shard.Context,
	cache wcache.Cache,
	config *configs.Config,
	timeSource clock.TimeSource,
	visibilityManager manager.VisibilityManager,
) *DeleteManagerImpl {
	deleteManager := &DeleteManagerImpl{
		shardContext:      shardContext,
		workflowCache:     cache,
		metricsHandler:    shardContext.GetMetricsHandler(),
		config:            config,
		timeSource:        timeSource,
		visibilityManager: visibilityManager,
	}

	return deleteManager
}

func (m *DeleteManagerImpl) AddDeleteWorkflowExecutionTask(
	ctx context.Context,
	nsID namespace.ID,
	we *commonpb.WorkflowExecution,
	ms workflow.MutableState,
) error {

	taskGenerator := workflow.NewTaskGeneratorProvider().NewTaskGenerator(m.shardContext, ms)

	// We can make this task immediately because the task itself will keep rescheduling itself until the workflow is
	// closed before actually deleting the workflow.
	deleteTask, err := taskGenerator.GenerateDeleteExecutionTask()
	if err != nil {
		return err
	}

	return m.shardContext.AddTasks(ctx, &persistence.AddHistoryTasksRequest{
		ShardID: m.shardContext.GetShardID(),
		// RangeID is set by shardContext
		NamespaceID: nsID.String(),
		WorkflowID:  we.GetWorkflowId(),
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer: {deleteTask},
		},
	})
}

func (m *DeleteManagerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	nsID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx workflow.Context,
	ms workflow.MutableState,
	forceDeleteFromOpenVisibility bool,
	stage *tasks.DeleteWorkflowExecutionStage,
) error {

	return m.deleteWorkflowExecutionInternal(ctx, nsID, we, weCtx, ms, forceDeleteFromOpenVisibility, stage, m.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryDeleteWorkflowExecutionScope)))
}

func (m *DeleteManagerImpl) DeleteWorkflowExecutionByRetention(
	ctx context.Context,
	nsID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx workflow.Context,
	ms workflow.MutableState,
	stage *tasks.DeleteWorkflowExecutionStage,
) error {

	return m.deleteWorkflowExecutionInternal(ctx, nsID, we, weCtx, ms, false, stage, m.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryProcessDeleteHistoryEventScope)))
}

func (m *DeleteManagerImpl) deleteWorkflowExecutionInternal(
	ctx context.Context,
	namespaceID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx workflow.Context,
	ms workflow.MutableState,
	forceDeleteFromOpenVisibility bool, //revive:disable-line:flag-parameter
	stage *tasks.DeleteWorkflowExecutionStage,
	metricsHandler metrics.Handler,
) error {

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	if err := m.shardContext.DeleteWorkflowExecution(
		ctx,
		definition.WorkflowKey{
			NamespaceID: namespaceID.String(),
			WorkflowID:  we.GetWorkflowId(),
			RunID:       we.GetRunId(),
		},
		currentBranchToken,
		ms.GetExecutionInfo().GetCloseVisibilityTaskId(),
		stage,
	); err != nil {
		return err
	}

	// Clear workflow execution context here to prevent further readers to get stale copy of non-exiting workflow execution.
	weCtx.Clear()

	metrics.WorkflowCleanupDeleteCount.With(metricsHandler).Record(1)
	return nil
}
