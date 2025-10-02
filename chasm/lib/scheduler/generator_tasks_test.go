package scheduler_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
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
	require.NoError(s.T(), err)
	err = s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.True(s.T(), common.IsInternalError(err))
}

func (s *generatorTasksSuite) TestExecuteBufferTask_Basic() {
	ctx := s.newMutableContext()
	sched := s.scheduler

	generator, err := sched.Generator.Get(ctx)
	require.NoError(s.T(), err)

	// Use a real SpecProcessor implementation.
	specProcessor := newTestSpecProcessor(s.controller)
	s.executor.SpecProcessor = specProcessor

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	highWatermark := ctx.Now(generator).UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the generate task.
	err = s.executor.Execute(ctx, generator, chasm.TaskAttributes{}, &schedulerpb.GeneratorTask{})
	require.NoError(s.T(), err)

	// We expect 5 buffered starts.
	invoker, err := sched.Invoker.Get(ctx)
	require.NoError(s.T(), err)
	require.Equal(s.T(), 5, len(invoker.BufferedStarts))

	// Generator's high water mark should have advanced.
	newHighWatermark := generator.LastProcessedTime.AsTime()
	require.True(s.T(), newHighWatermark.After(highWatermark))

	// Ensure we scheduled an immediate physical pure task on the tree.
	_, err = s.node.CloseTransaction()
	require.NoError(s.T(), err)
	require.True(s.T(), s.hasTask(&tasks.ChasmTaskPure{}, chasm.TaskScheduledTimeImmediate))
}
