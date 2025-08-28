package scheduler_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
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
	s.SetupSuite()
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
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, errors.New("processTimeRange bug"))

	// Execute the generate task.
	generator, err := sched.Generator.Get(ctx)
	s.NoError(err)
	err = s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.True(common.IsInternalError(err))
}

func (s *generatorTasksSuite) TestExecuteBufferTask_Basic() {
	ctx := s.newMutableContext()
	sched := s.scheduler

	generator, err := sched.Generator.Get(ctx)
	s.NoError(err)

	// Use a real SpecProcessor implementation.
	specProcessor := newTestSpecProcessor(s.controller)
	s.executor.SpecProcessor = specProcessor

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the generate task.
	err = s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	s.NoError(err)

	// We expect 5 buffered starts.
	invoker, err := sched.Invoker.Get(ctx)
	s.NoError(err)
	s.Equal(5, len(invoker.BufferedStarts))

	// Generator's high water mark should have advanced.
	newHighWatermark := generator.LastProcessedTime.AsTime()
	s.True(newHighWatermark.After(highWatermark))

	// Ensure we scheduled an immediate physical pure task on the tree.
	_, err = s.node.CloseTransaction()
	s.NoError(err)
	s.Equal(1, len(s.addedTasks))
	task, ok := s.addedTasks[0].(*tasks.ChasmTaskPure)
	s.True(ok)
	s.Equal(chasm.TaskScheduledTimeImmediate, task.GetVisibilityTime())
}
