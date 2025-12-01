package scheduler_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	queueerrors "go.temporal.io/server/service/history/queues/errors"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type generatorTasksSuite struct {
	schedulerSuite
	executor *scheduler.GeneratorTaskExecutor
}

func TestGeneratorTasksSuite(t *testing.T) {
	suite.Run(t, &generatorTasksSuite{})
}

func (s *generatorTasksSuite) SetupTest() {
	s.schedulerSuite.SetupTest()
	s.executor = scheduler.NewGeneratorTaskExecutor(scheduler.GeneratorTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     s.logger,
		SpecProcessor:  s.specProcessor,
	})
}

func (s *generatorTasksSuite) TestExecute_ProcessTimeRangeFails() {
	sched := s.scheduler
	ctx := s.newMutableContext()

	// If ProcessTimeRange fails, we should fail the task as an internal error.
	s.specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, errors.New("processTimeRange bug"))

	// Execute the generate task.
	generator := sched.Generator.Get(ctx)
	err := s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	var target *queueerrors.UnprocessableTaskError
	s.ErrorAs(err, &target)
	s.Equal("failed to process a time range: processTimeRange bug", target.Message)
}

func (s *generatorTasksSuite) TestExecuteBufferTask_Basic() {
	ctx := s.newMutableContext()
	sched := s.scheduler

	generator := sched.Generator.Get(ctx)

	// Use a real SpecProcessor implementation.
	specProcessor := newTestSpecProcessor(s.controller)
	s.executor.SpecProcessor = specProcessor

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the generate task.
	err := s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.NoError(err)

	// We expect 5 buffered starts.
	invoker := sched.Invoker.Get(ctx)
	s.Equal(5, len(invoker.BufferedStarts))

	// Validate RequestId -> WorkflowId mapping
	for _, start := range invoker.BufferedStarts {
		s.Equal(start.WorkflowId, invoker.WorkflowID(start.RequestId))
	}

	// Generator's high water mark should have advanced.
	newHighWatermark := generator.LastProcessedTime.AsTime()
	s.True(newHighWatermark.After(highWatermark))

	// Ensure we scheduled an immediate physical pure task on the tree.
	_, err = s.node.CloseTransaction()
	s.NoError(err)
	s.True(s.hasTask(&tasks.ChasmTaskPure{}, chasm.TaskScheduledTimeImmediate))
}

func (s *generatorTasksSuite) TestUpdateFutureActionTimes_UnlimitedActions() {
	ctx := s.newMutableContext()
	sched := s.scheduler
	generator := sched.Generator.Get(ctx)

	s.executor.SpecProcessor = newTestSpecProcessor(s.controller)

	err := s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.NoError(err)

	s.NotEmpty(generator.FutureActionTimes)
	s.Require().Len(generator.FutureActionTimes, 10)
}

func (s *generatorTasksSuite) TestUpdateFutureActionTimes_LimitedActions() {
	ctx := s.newMutableContext()
	sched := s.scheduler
	generator := sched.Generator.Get(ctx)

	sched.Schedule.State.LimitedActions = true
	sched.Schedule.State.RemainingActions = 2
	s.executor.SpecProcessor = newTestSpecProcessor(s.controller)

	err := s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.NoError(err)

	s.Len(generator.FutureActionTimes, 2)
}

func (s *generatorTasksSuite) TestUpdateFutureActionTimes_SkipsBeforeUpdateTime() {
	ctx := s.newMutableContext()
	sched := s.scheduler
	generator := sched.Generator.Get(ctx)

	s.executor.SpecProcessor = newTestSpecProcessor(s.controller)

	// UpdateTime acts as a floor - action times at or before it are skipped.
	baseTime := ctx.Now(generator).UTC()
	updateTime := baseTime.Add(defaultInterval / 2)
	sched.Info.UpdateTime = timestamppb.New(updateTime)

	err := s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.NoError(err)

	s.Require().NotEmpty(generator.FutureActionTimes)
	for _, futureTime := range generator.FutureActionTimes {
		s.True(futureTime.AsTime().After(updateTime))
	}
}
