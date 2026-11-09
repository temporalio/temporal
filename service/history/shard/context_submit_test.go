package shard

import (
	"context"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type pendingUpdateReceipt struct {
	done chan struct{}
	err  error
}

func newPendingUpdateReceipt() *pendingUpdateReceipt {
	return &pendingUpdateReceipt{done: make(chan struct{})}
}

func (r *pendingUpdateReceipt) complete(err error) {
	r.err = err
	close(r.done)
}

func (r *pendingUpdateReceipt) Wait(ctx context.Context) error {
	select {
	case <-r.done:
		return r.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *contextSuite) newSubmitRequest() (*persistence.UpdateWorkflowExecutionRequest, tasks.Task) {
	task := &tasks.ActivityTask{
		WorkflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID),
	}
	return &persistence.UpdateWorkflowExecutionRequest{
		ShardID:     s.shardID,
		Mode:        persistence.UpdateWorkflowModeUpdateCurrent,
		ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: persistence.WorkflowMutation{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: tests.NamespaceID.String(), WorkflowId: tests.WorkflowID},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: tests.RunID},
			Tasks:          map[tasks.Category][]tasks.Task{tasks.CategoryTransfer: {task}},
		},
	}, task
}

func (s *contextSuite) TestSubmitWorkflowExecutionUpdate_ReadLevelWaitsForReceipt() {
	request, task := s.newSubmitRequest()
	inner := newPendingUpdateReceipt()
	s.mockExecutionManager.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), request).
		Return(&persistence.UpdateWorkflowExecutionResponse{}, inner, nil)

	_, receipt, err := s.mockShard.SubmitWorkflowExecutionUpdate(context.Background(), request)
	s.NoError(err)
	s.Equal(s.mockShard.GetRangeID(), request.RangeID)
	s.NotZero(task.GetTaskID())
	s.LessOrEqual(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTransfer).TaskID, task.GetTaskID())

	inner.complete(nil)
	s.NoError(receipt.Wait(context.Background()))
	s.Greater(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTransfer).TaskID, task.GetTaskID())
}

func (s *contextSuite) TestSubmitWorkflowExecutionUpdate_DefiniteFailureReleasesReadLevel() {
	request, task := s.newSubmitRequest()
	inner := newPendingUpdateReceipt()
	s.mockExecutionManager.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), request).
		Return(&persistence.UpdateWorkflowExecutionResponse{}, inner, nil)

	_, receipt, err := s.mockShard.SubmitWorkflowExecutionUpdate(context.Background(), request)
	s.NoError(err)
	inner.complete(&persistence.WorkflowConditionFailedError{Msg: "conflict"})
	var conditionFailed *persistence.WorkflowConditionFailedError
	s.ErrorAs(receipt.Wait(context.Background()), &conditionFailed)
	s.Greater(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTransfer).TaskID, task.GetTaskID())
	s.True(s.mockShard.IsValid())
}

func (s *contextSuite) TestSubmitWorkflowExecutionUpdate_SubmitFailure() {
	request, task := s.newSubmitRequest()
	s.mockExecutionManager.EXPECT().SubmitWorkflowExecutionUpdate(gomock.Any(), request).
		Return(nil, nil, &persistence.ConditionFailedError{Msg: "conflict"})

	_, receipt, err := s.mockShard.SubmitWorkflowExecutionUpdate(context.Background(), request)
	var conditionFailed *persistence.ConditionFailedError
	s.ErrorAs(err, &conditionFailed)
	s.Nil(receipt)
	s.Greater(s.mockShard.GetQueueExclusiveHighReadWatermark(tasks.CategoryTransfer).TaskID, task.GetTaskID())
}

func (s *contextSuite) TestSubmitWorkflowExecutionUpdate_RejectsNewRun() {
	request, _ := s.newSubmitRequest()
	request.NewWorkflowSnapshot = &persistence.WorkflowSnapshot{}

	_, _, err := s.mockShard.SubmitWorkflowExecutionUpdate(context.Background(), request)
	var invalid *persistence.InvalidPersistenceRequestError
	s.ErrorAs(err, &invalid)
}
