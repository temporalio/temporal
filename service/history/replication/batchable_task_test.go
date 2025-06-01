package replication

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

type (
	batchedTaskSuite struct {
		suite.Suite
		*require.Assertions
		controller     *gomock.Controller
		logger         log.Logger
		metricsHandler metrics.Handler
	}
)

func (s *batchedTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.metricsHandler = metrics.NoopMetricsHandler
}

func TestBatchedTaskSuite(t *testing.T) {
	suite.Run(t, new(batchedTaskSuite))
}

func (s *batchedTaskSuite) TestAddTask_batchStateClose_DoNotBatch_ReturnFalse() {
	incomingTask := NewMockTrackableExecutableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     incomingTask,
		individualTasks: append([]TrackableExecutableTask{}, incomingTask),
		state:           batchStateClose,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	result := batchedTestTask.AddTask(NewMockTrackableExecutableTask(s.controller))
	s.False(result)
}

func (s *batchedTaskSuite) TestAddTask_ExistingTaskIsNotBatchable_DoNotBatch_ReturnFalse() {
	existing := NewMockTrackableExecutableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockTrackableExecutableTask(s.controller)
	result := batchedTestTask.AddTask(incoming)
	s.False(result)
}

func (s *batchedTaskSuite) TestAddTask_IncomingTaskIsNotBatchable_DoNotBatch_ReturnFalse() {
	existing := NewMockTrackableExecutableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(s.controller)
	result := batchedTestTask.AddTask(incoming)
	s.False(result)
}

func (s *batchedTaskSuite) TestAddTask_ExistingTaskDoesNotWantToBatch_DoNotBatch_ReturnFalse() {
	existing := NewMockBatchableTask(s.controller)
	existing.EXPECT().CanBatch().Return(false).Times(1)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(s.controller)
	incoming.EXPECT().CanBatch().Return(true).Times(1)

	result := batchedTestTask.AddTask(incoming)
	s.False(result)
}

func (s *batchedTaskSuite) TestAddTask_IncomingTaskDoesNotWantToBatch_DoNotBatch_ReturnFalse() {
	existing := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(s.controller)
	incoming.EXPECT().CanBatch().Return(false).Times(1)

	result := batchedTestTask.AddTask(incoming)
	s.False(result)
}

func (s *batchedTaskSuite) TestAddTask_TasksAreBatchableAndCanBatch_ReturnTrue() {
	existing := NewMockBatchableTask(s.controller)
	existing.EXPECT().CanBatch().Return(true).Times(1)

	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	incoming := NewMockBatchableTask(s.controller)
	incoming.EXPECT().CanBatch().Return(true).Times(1)

	batchResult := NewMockTrackableExecutableTask(s.controller)
	existing.EXPECT().BatchWith(incoming).Return(batchResult, true).Times(1)
	result := batchedTestTask.AddTask(incoming)

	s.True(result)

	// verify individual tasks
	s.True(batchResult == batchedTestTask.batchedTask)
	s.Len(batchedTestTask.individualTasks, 2)
	s.True(existing == batchedTestTask.individualTasks[0])
	s.True(incoming == batchedTestTask.individualTasks[1])
}

func (s *batchedTaskSuite) TestExecute_SetBatchStateToClose_ReturnResult() {
	existing := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	err := errors.New("some error")
	existing.EXPECT().Execute().Return(err).Times(1)
	result := batchedTestTask.Execute()

	s.Equal(batchState(batchStateClose), batchedTestTask.state)
	s.Equal(err, result)
}

func (s *batchedTaskSuite) TestAck_AckIndividualTasks() {
	existing := NewMockBatchableTask(s.controller)
	add1 := NewMockBatchableTask(s.controller)
	add2 := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
		metricsHandler: s.metricsHandler,
	}
	existing.EXPECT().Ack().Times(1)
	add1.EXPECT().Ack().Times(1)
	add2.EXPECT().Ack().Times(1)

	batchedTestTask.Ack()
}

func (s *batchedTaskSuite) TestAbort_AbortIndividualTasks() {
	existing := NewMockBatchableTask(s.controller)
	add1 := NewMockBatchableTask(s.controller)
	add2 := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Abort().Times(1)
	add1.EXPECT().Abort().Times(1)
	add2.EXPECT().Abort().Times(1)

	batchedTestTask.Abort()
}

func (s *batchedTaskSuite) TestCancel_CancelIndividualTasks() {
	existing := NewMockBatchableTask(s.controller)
	add1 := NewMockBatchableTask(s.controller)
	add2 := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Cancel().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add2.EXPECT().Cancel().Times(1)

	batchedTestTask.Cancel()
}

func (s *batchedTaskSuite) TestNack_SingleItem_NackTheTask() {
	existing := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Nack(nil).Times(1)

	batchedTestTask.Nack(nil)
}

func (s *batchedTaskSuite) TestNack_MultipleItems_CallIndividualHandler() {
	existing := NewMockBatchableTask(s.controller)
	add1 := NewMockBatchableTask(s.controller)
	add2 := NewMockBatchableTask(s.controller)
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			task.Cancel()
			task.Abort()
			task.Reschedule()
		},
		logger: s.logger,
	}
	existing.EXPECT().Cancel().Times(1)
	existing.EXPECT().MarkUnbatchable().Times(1)
	existing.EXPECT().Abort().Times(1)
	existing.EXPECT().Reschedule().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add1.EXPECT().Abort().Times(1)
	add1.EXPECT().Reschedule().Times(1)
	add1.EXPECT().MarkUnbatchable().Times(1)

	add2.EXPECT().Cancel().Times(1)
	add2.EXPECT().Abort().Times(1)
	add2.EXPECT().Reschedule().Times(1)
	add2.EXPECT().MarkUnbatchable().Times(1)

	batchedTestTask.Nack(nil)
}

func (s *batchedTaskSuite) TestMarkPoisonPill_SingleItem_MarkTheTask() {
	existing := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().MarkPoisonPill().Return(nil).Times(1)

	result := batchedTestTask.MarkPoisonPill()
	s.Nil(result)
}

func (s *batchedTaskSuite) TestReschedule_SingleItem_RescheduleTheTask() {
	existing := NewMockBatchableTask(s.controller)
	handlerCallCount := 0
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			handlerCallCount++
		},
	}
	existing.EXPECT().Reschedule().Times(1)

	batchedTestTask.Reschedule()
	s.Equal(0, handlerCallCount)
}

func (s *batchedTaskSuite) TestMarkPoisonPill_MultipleItems_CallIndividualHandler() {
	existing := NewMockBatchableTask(s.controller)
	add1 := NewMockBatchableTask(s.controller)
	add2 := NewMockBatchableTask(s.controller)
	batchedTestTask := &batchedTask{
		batchedTask:     existing,
		individualTasks: append([]TrackableExecutableTask{}, existing, add1, add2),
		state:           batchStateOpen,
		individualTaskHandler: func(task TrackableExecutableTask) {
			task.Cancel()
			task.Abort()
			task.Reschedule()
		},
		logger: s.logger,
	}
	existing.EXPECT().Cancel().Times(1)
	existing.EXPECT().MarkUnbatchable().Times(1)
	existing.EXPECT().Abort().Times(1)
	existing.EXPECT().Reschedule().Times(1)
	add1.EXPECT().Cancel().Times(1)
	add1.EXPECT().Abort().Times(1)
	add1.EXPECT().Reschedule().Times(1)
	add1.EXPECT().MarkUnbatchable().Times(1)

	add2.EXPECT().Cancel().Times(1)
	add2.EXPECT().Abort().Times(1)
	add2.EXPECT().Reschedule().Times(1)
	add2.EXPECT().MarkUnbatchable().Times(1)

	result := batchedTestTask.MarkPoisonPill()
	s.Nil(result)
}
