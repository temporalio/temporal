package testcore

import (
	"context"

	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
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
		m.recordWorkflowSnapshot(request.ShardID, request.RangeID, &request.NewWorkflowSnapshot)
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.UpdateWorkflowExecutionRequest,
) (*persistence.UpdateWorkflowExecutionResponse, error) {
	response, err := m.ExecutionManager.UpdateWorkflowExecution(ctx, request)
	if err == nil && request != nil {
		m.recordWorkflowMutation(request.ShardID, request.RangeID, &request.UpdateWorkflowMutation)
		m.recordWorkflowSnapshot(request.ShardID, request.RangeID, request.NewWorkflowSnapshot)
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.ConflictResolveWorkflowExecutionRequest,
) (*persistence.ConflictResolveWorkflowExecutionResponse, error) {
	response, err := m.ExecutionManager.ConflictResolveWorkflowExecution(ctx, request)
	if err == nil && request != nil {
		m.recordWorkflowSnapshot(request.ShardID, request.RangeID, &request.ResetWorkflowSnapshot)
		m.recordWorkflowSnapshot(request.ShardID, request.RangeID, request.NewWorkflowSnapshot)
		m.recordWorkflowMutation(request.ShardID, request.RangeID, request.CurrentWorkflowMutation)
	}
	return response, err
}

func (m *taskQueueRecordingExecutionManager) recordWorkflowSnapshot(
	shardID int32,
	rangeID int64,
	snapshot *persistence.WorkflowSnapshot,
) {
	if snapshot == nil || snapshot.ExecutionInfo == nil {
		return
	}
	m.recorder.Record(
		shardID,
		rangeID,
		snapshot.ExecutionInfo.NamespaceId,
		snapshot.ExecutionInfo.WorkflowId,
		snapshot.Tasks,
	)
}

func (m *taskQueueRecordingExecutionManager) recordWorkflowMutation(
	shardID int32,
	rangeID int64,
	mutation *persistence.WorkflowMutation,
) {
	if mutation == nil || mutation.ExecutionInfo == nil {
		return
	}
	m.recorder.Record(
		shardID,
		rangeID,
		mutation.ExecutionInfo.NamespaceId,
		mutation.ExecutionInfo.WorkflowId,
		mutation.Tasks,
	)
}

func (c *TemporalImpl) historyFactoryProvider(
	params persistenceClient.NewFactoryParams,
) persistenceClient.Factory {
	factory := persistenceClient.FactoryProvider(params)
	if c.taskQueueRecorder == nil {
		return factory
	}
	return &taskQueueRecordingFactory{
		Factory:  factory,
		recorder: c.taskQueueRecorder,
	}
}
