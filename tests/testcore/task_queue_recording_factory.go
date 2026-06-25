package testcore

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/service/history/tasks"
)

type taskQueueRecordingFactory struct {
	persistenceClient.Factory
	recorder *TaskQueueRecorder
}

func (f *taskQueueRecordingFactory) NewExecutionManager() (persistence.ExecutionManager, error) {
	manager, err := f.Factory.NewExecutionManager()
	if err != nil {
		return nil, err
	}
	return &taskQueueRecordingExecutionManager{
		ExecutionManager: manager,
		recorder:         f.recorder,
	}, nil
}

type taskQueueRecordingExecutionManager struct {
	persistence.ExecutionManager
	recorder *TaskQueueRecorder
}

func (m *taskQueueRecordingExecutionManager) AddHistoryTasks(
	ctx context.Context,
	request *persistence.AddHistoryTasksRequest,
) error {
	err := m.ExecutionManager.AddHistoryTasks(ctx, request)
	if err == nil && request != nil {
		m.recorder.Record(request.ShardID, request.RangeID, request.NamespaceID, request.WorkflowID, request.Tasks)
	}
	return err
}

func (m *taskQueueRecordingExecutionManager) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.CreateWorkflowExecutionRequest,
) (*persistence.CreateWorkflowExecutionResponse, error) {
	response, err := m.ExecutionManager.CreateWorkflowExecution(ctx, request)
	if err == nil && request != nil {
		m.recordWorkflowTasks(
			request.ShardID,
			request.RangeID,
			request.NewWorkflowSnapshot.ExecutionInfo,
			request.NewWorkflowSnapshot.Tasks,
		)
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	response, err := m.ExecutionManager.UpdateWorkflowExecution(ctx, request)
	if err == nil && request != nil {
		m.recordWorkflowTasks(
			request.ShardID,
			request.RangeID,
			request.UpdateWorkflowMutation.ExecutionInfo,
			request.UpdateWorkflowMutation.Tasks,
		)
		if request.NewWorkflowSnapshot != nil {
			m.recordWorkflowTasks(
				request.ShardID,
				request.RangeID,
				request.NewWorkflowSnapshot.ExecutionInfo,
				request.NewWorkflowSnapshot.Tasks,
			)
		}
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	response, err := m.ExecutionManager.ConflictResolveWorkflowExecution(ctx, request)
	if err == nil && request != nil {
		m.recordWorkflowTasks(
			request.ShardID,
			request.RangeID,
			request.ResetWorkflowSnapshot.ExecutionInfo,
			request.ResetWorkflowSnapshot.Tasks,
		)
		if request.NewWorkflowSnapshot != nil {
			m.recordWorkflowTasks(
				request.ShardID,
				request.RangeID,
				request.NewWorkflowSnapshot.ExecutionInfo,
				request.NewWorkflowSnapshot.Tasks,
			)
		}
		if request.CurrentWorkflowMutation != nil {
			m.recordWorkflowTasks(
				request.ShardID,
				request.RangeID,
				request.CurrentWorkflowMutation.ExecutionInfo,
				request.CurrentWorkflowMutation.Tasks,
			)
		}
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) recordWorkflowTasks(
	shardID int32,
	rangeID int64,
	info *persistencespb.WorkflowExecutionInfo,
	tasksByCategory map[tasks.Category][]tasks.Task,
) {
	m.recorder.Record(
		shardID,
		rangeID,
		info.NamespaceId,
		info.WorkflowId,
		tasksByCategory,
	)
}
