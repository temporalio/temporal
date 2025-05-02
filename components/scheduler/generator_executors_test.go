package scheduler_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// registerGeneratorExecutor creates and registers an executor with dependencies injected for testing.
func registerGeneratorExecutor(t *testing.T, ctrl *gomock.Controller, registry *hsm.Registry, specProcessor scheduler.SpecProcessor) {
	require.NoError(t, scheduler.RegisterGeneratorExecutors(registry, scheduler.GeneratorTaskExecutorOptions{
		Config:         &scheduler.Config{},
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewTestLogger(),
		SpecProcessor:  specProcessor,
	}))
}

func TestExecuteBufferTask_ProcessTimeRangeFails(t *testing.T) {
	env := newFakeEnv()
	registry := newRegistry(t)
	ctrl := gomock.NewController(t)
	backend := &hsmtest.NodeBackend{}
	root := newRoot(t, registry, backend)
	schedulerNode := newSchedulerTree(t, registry, root, defaultSchedule(), nil)

	// If ProcessTimeRange fails, we should fail the task as an internal error.
	specProcessor := scheduler.NewMockSpecProcessor(ctrl)
	specProcessor.EXPECT().ProcessTimeRange(
		gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
	).Return(nil, errors.New("processTimeRange bug"))

	registerGeneratorExecutor(t, ctrl, registry, specProcessor)
	generatorNode, err := schedulerNode.Child([]hsm.Key{scheduler.GeneratorMachineKey})
	require.NoError(t, err)

	// Execute the buffer task.
	err = registry.ExecuteTimerTask(env, generatorNode, scheduler.BufferTask{})
	require.True(t, common.IsInternalError(err))
}

func TestExecuteBufferTask_Basic(t *testing.T) {
	env := newFakeEnv()
	registry := newRegistry(t)
	ctrl := gomock.NewController(t)
	backend := &hsmtest.NodeBackend{}
	root := newRoot(t, registry, backend)
	schedulerNode := newSchedulerTree(t, registry, root, defaultSchedule(), nil)

	// Use a real SpecProcessor.
	specProcessor := newTestSpecProcessor(ctrl)
	registerGeneratorExecutor(t, ctrl, registry, specProcessor.SpecProcessor)
	generatorNode, err := schedulerNode.Child([]hsm.Key{scheduler.GeneratorMachineKey})
	require.NoError(t, err)

	// Move high water mark back in time (Generator always compares high water mark
	// against system time) to generate buffered actions.
	generator, err := hsm.MachineData[scheduler.Generator](generatorNode)
	require.NoError(t, err)
	highWatermark := env.Now().UTC().Add(-defaultInterval * 5)
	generator.LastProcessedTime = timestamppb.New(highWatermark)

	// Execute the buffer task.
	err = registry.ExecuteTimerTask(env, generatorNode, scheduler.BufferTask{})
	require.NoError(t, err)

	// Buffering should have resulted in buffered starts being applied to the
	// Invoker.
	invokerNode, err := schedulerNode.Child([]hsm.Key{scheduler.InvokerMachineKey})
	require.NoError(t, err)
	invoker, err := hsm.MachineData[scheduler.Invoker](invokerNode)
	require.NoError(t, err)

	// We expect 5 buffered starts.
	require.Equal(t, 5, len(invoker.BufferedStarts))

	// Generator's high water mark should have advanced.
	generator, err = hsm.MachineData[scheduler.Generator](generatorNode)
	require.NoError(t, err)
	newHighWatermark := generator.LastProcessedTime.AsTime()
	require.True(t, newHighWatermark.After(highWatermark))
	require.True(t, generator.NextInvocationTime.AsTime().After(newHighWatermark))

	// We should have enqueued a ProcessBuffer task on Invoker, and another Buffer
	// task on Generator.
	opLog, err := root.OpLog()
	require.NoError(t, err)
	require.Equal(t, 2, len(opLog))

	// The ProcessBuffer task should be scheduled immediately.
	output, ok := opLog[0].(hsm.TransitionOperation)
	require.True(t, ok)
	require.Equal(t, 1, len(output.Output.Tasks))
	task := output.Output.Tasks[0]
	require.Equal(t, scheduler.TaskTypeProcessBuffer, task.Type())
	require.Equal(t, hsm.Immediate, task.Deadline())

	// The Buffer task should have a deadline on our next invocation time.
	output, ok = opLog[1].(hsm.TransitionOperation)
	require.True(t, ok)
	require.Equal(t, 1, len(output.Output.Tasks))
	task = output.Output.Tasks[0]
	require.Equal(t, scheduler.TaskTypeBuffer, task.Type())
	require.Equal(t, generator.NextInvocationTime.AsTime(), task.Deadline())
}
