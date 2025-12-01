//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination delete_manager_mock.go

package deletemanager

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
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
			ms historyi.MutableState,
		) error
		DeleteWorkflowExecution(
			ctx context.Context,
			nsID namespace.ID,
			we *commonpb.WorkflowExecution,
			weCtx historyi.WorkflowContext,
			ms historyi.MutableState,
			stage *tasks.DeleteWorkflowExecutionStage,
		) error
		DeleteWorkflowExecutionByRetention(
			ctx context.Context,
			nsID namespace.ID,
			we *commonpb.WorkflowExecution,
			weCtx historyi.WorkflowContext,
			ms historyi.MutableState,
			stage *tasks.DeleteWorkflowExecutionStage,
		) error
	}

	DeleteManagerImpl struct {
		shardContext      historyi.ShardContext
		workflowCache     wcache.Cache
		config            *configs.Config
		metricsHandler    metrics.Handler
		timeSource        clock.TimeSource
		visibilityManager manager.VisibilityManager
	}
)

var _ DeleteManager = (*DeleteManagerImpl)(nil)

func NewDeleteManager(
	shardContext historyi.ShardContext,
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
	ms historyi.MutableState,
) error {

	taskGenerator := workflow.GetTaskGeneratorProvider().NewTaskGenerator(m.shardContext, ms)

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
		ArchetypeID: chasm.WorkflowArchetypeID, // this method is specific to workflow executions
		Tasks: map[tasks.Category][]tasks.Task{
			tasks.CategoryTransfer: {deleteTask},
		},
	})
}

func (m *DeleteManagerImpl) DeleteWorkflowExecution(
	ctx context.Context,
	nsID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx historyi.WorkflowContext,
	ms historyi.MutableState,
	stage *tasks.DeleteWorkflowExecutionStage,
) error {

	return m.deleteWorkflowExecutionInternal(ctx, nsID, we, weCtx, ms, stage, m.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryDeleteWorkflowExecutionScope)))
}

func (m *DeleteManagerImpl) DeleteWorkflowExecutionByRetention(
	ctx context.Context,
	nsID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx historyi.WorkflowContext,
	ms historyi.MutableState,
	stage *tasks.DeleteWorkflowExecutionStage,
) error {

	return m.deleteWorkflowExecutionInternal(ctx, nsID, we, weCtx, ms, stage, m.metricsHandler.WithTags(metrics.OperationTag(metrics.HistoryProcessDeleteHistoryEventScope)))
}

func (m *DeleteManagerImpl) deleteWorkflowExecutionInternal(
	ctx context.Context,
	namespaceID namespace.ID,
	we *commonpb.WorkflowExecution,
	weCtx historyi.WorkflowContext,
	ms historyi.MutableState,
	stage *tasks.DeleteWorkflowExecutionStage,
	metricsHandler metrics.Handler,
) error {

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return err
	}

	executionInfo := ms.GetExecutionInfo()
	if err := m.shardContext.DeleteWorkflowExecution(
		ctx,
		definition.WorkflowKey{
			NamespaceID: namespaceID.String(),
			WorkflowID:  we.GetWorkflowId(),
			RunID:       we.GetRunId(),
		},
		ms.ChasmTree().ArchetypeID(),
		currentBranchToken,
		executionInfo.GetCloseVisibilityTaskId(),
		executionInfo.GetCloseTime().AsTime(),
		stage,
	); err != nil {
		return err
	}

	// Clear workflow execution context here to prevent further readers to get stale copy of non-exiting workflow execution.
	weCtx.Clear()

	metrics.WorkflowCleanupDeleteCount.With(metricsHandler).Record(1)
	return nil
}
