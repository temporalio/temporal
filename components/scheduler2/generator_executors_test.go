package scheduler2_test

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/scheduler2"
	"go.temporal.io/server/service/history/hsm"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// registerGeneratorExecutor creates and registers an executor with dependencies injected for testing.
func registerGeneratorExecutor(t *testing.T, ctrl *gomock.Controller, registry *hsm.Registry, specProcessor scheduler2.SpecProcessor) {
	require.NoError(t, scheduler2.RegisterGeneratorExecutors(registry, scheduler2.GeneratorTaskExecutorOptions{
		Config:         &scheduler2.Config{},
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewTestLogger(),
		SpecProcessor:  specProcessor,
	}))
}

func TestExecuteBufferTask_ProcessTimeRangeFails(t *testing.T) {
	env := fakeEnv{}
	registry := newRegistry(t)
	ctrl := gomock.NewController(t)
	root := newSchedulerTree(t, registry, defaultSchedule(), nil)

	// If ProcessTimeRange fails, we should fail the task as an internal error.
	specProcessor := scheduler2.NewMockSpecProcessor(ctrl)
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, errors.New("processTimeRange bug"))

	registerGeneratorExecutor(t, ctrl, registry, specProcessor)
	generatorNode, err := root.Child([]hsm.Key{scheduler2.GeneratorMachineKey})
	require.NoError(t, err)

	// Execute the buffer task.
	err = registry.ExecuteTimerTask(env, generatorNode, scheduler2.BufferTask{})
	require.True(t, common.IsInternalError(err))
}

func TestExecuteBufferTask_Basic(t *testing.T) {
	env := fakeEnv{}
	registry := newRegistry(t)
	ctrl := gomock.NewController(t)
	root := newSchedulerTree(t, registry, defaultSchedule(), nil)

	// Use a real SpecProcessor.
	specProcessor := newTestSpecProcessor(ctrl)
	registerGeneratorExecutor(t, ctrl, registry, specProcessor.SpecProcessor)
	generatorNode, err := root.Child([]hsm.Key{scheduler2.GeneratorMachineKey})
	require.NoError(t, err)

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	generator, err := hsm.MachineData[scheduler2.Generator](generatorNode)
	require.NoError(t, err)
	highWatermark := time.Now().UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the buffer task.
	err = registry.ExecuteTimerTask(env, generatorNode, scheduler2.BufferTask{})
	require.NoError(t, err)

	// Buffering should have resulted in buffered starts being applied to the
	// Executor.
	executorNode, err := root.Child([]hsm.Key{scheduler2.ExecutorMachineKey})
	require.NoError(t, err)
	executor, err := hsm.MachineData[scheduler2.Executor](executorNode)
	require.NoError(t, err)

	// We expect 5 buffered starts, but the executor uses time.Now(), so GTE to be safe.
	require.GreaterOrEqual(t, 5, len(executor.BufferedStarts))

	// Generator's high water mark should have advanced.
	generator, err = hsm.MachineData[scheduler2.Generator](generatorNode)
	require.NoError(t, err)
	newHighWatermark := generator.LastProcessedTime.AsTime()
	require.True(t, newHighWatermark.After(highWatermark))
	require.True(t, generator.NextInvocationTime.AsTime().After(newHighWatermark))

	// A single task should have been enqueued.
	outputs := generatorNode.Outputs()
	require.Equal(t, 1, len(outputs))
	require.Equal(t, 1, len(outputs[0].Outputs))
	require.Equal(t, 1, len(outputs[0].Outputs[0].Tasks))

	// The buffer task should have a deadline on our next invocation time.
	task := outputs[0].Outputs[0].Tasks[0]
	require.Equal(t, scheduler2.TaskTypeBuffer, task.Type())
	require.Equal(t, generator.NextInvocationTime.AsTime(), task.Deadline())
}
