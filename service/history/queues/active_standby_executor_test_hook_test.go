//go:build test_dep

package queues

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/service/history/consts"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

type activeStandbyPassiveReplicationTestHook struct{}

func (activeStandbyPassiveReplicationTestHook) InterceptUpdate(
	_ context.Context,
	_ any,
	next func() error,
) error {
	return next()
}

func (activeStandbyPassiveReplicationTestHook) UseTransientWorkflowContextForReplication(context.Context) bool {
	return false
}

func (activeStandbyPassiveReplicationTestHook) ShouldExecuteTaskAsPassive(historytasks.Task) bool {
	return true
}

func TestActiveStandbyExecutor_ForcedStandbyRetryExecutesActiveThenStandby(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	activeExecutor := NewMockExecutor(ctrl)
	standbyExecutor := NewMockExecutor(ctrl)
	testHooks := testhooks.NewTestHooks()
	t.Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		activeStandbyPassiveReplicationTestHook{},
		namespace.ID("namespace_id"),
	))
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	executor := NewActiveStandbyExecutor(
		currentCluster,
		registry,
		activeExecutor,
		standbyExecutor,
		log.NewNoopLogger(),
		metricsHandler,
		testHooks,
	)

	task := historytasks.NewFakeTask(
		definition.NewWorkflowKey("namespace_id", "workflow_id", "run_id"),
		historytasks.CategoryTransfer,
		time.Time{},
	)
	executable := NewMockExecutable(ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(task)
	gomock.InOrder(
		standbyExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
			ExecutedAsActive: false,
			ExecutionErr:     consts.ErrTaskRetry,
		}),
		activeExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
			ExecutedAsActive: true,
		}),
		standbyExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
			ExecutedAsActive: false,
		}),
	)

	response := executor.Execute(context.Background(), executable)
	require.False(t, response.ExecutedAsActive)
	require.NoError(t, response.ExecutionErr)
	recordings := capture.Snapshot()[metrics.HistoryPassiveReplicationTestHookCounter.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "ActiveStandbyExecutor", recordings[0].Tags[metrics.OperationTagName])
}

func TestActiveStandbyExecutor_ActiveFailureSkipsSecondStandby(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	activeExecutor := NewMockExecutor(ctrl)
	standbyExecutor := NewMockExecutor(ctrl)
	testHooks := testhooks.NewTestHooks()
	t.Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		activeStandbyPassiveReplicationTestHook{},
		namespace.ID("namespace_id"),
	))
	executor := NewActiveStandbyExecutor(
		currentCluster,
		registry,
		activeExecutor,
		standbyExecutor,
		log.NewNoopLogger(),
		metrics.NoopMetricsHandler,
		testHooks,
	)

	task := historytasks.NewFakeTask(
		definition.NewWorkflowKey("namespace_id", "workflow_id", "run_id"),
		historytasks.CategoryTransfer,
		time.Time{},
	)
	executable := NewMockExecutable(ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(task)
	activeErr := errors.New("active execution failed")
	gomock.InOrder(
		standbyExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
			ExecutedAsActive: false,
			ExecutionErr:     consts.ErrTaskRetry,
		}),
		activeExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
			ExecutedAsActive: true,
			ExecutionErr:     activeErr,
		}),
	)

	response := executor.Execute(context.Background(), executable)
	require.True(t, response.ExecutedAsActive)
	require.ErrorIs(t, response.ExecutionErr, activeErr)
}

func TestActiveStandbyExecutor_ForcedStandbySuccessIsReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	registry := namespace.NewMockRegistry(ctrl)
	activeExecutor := NewMockExecutor(ctrl)
	standbyExecutor := NewMockExecutor(ctrl)
	testHooks := testhooks.NewTestHooks()
	t.Cleanup(testhooks.Set[testhooks.HistoryPassiveReplicationTestHook](
		testHooks,
		testhooks.HistoryPassiveReplicationTest,
		activeStandbyPassiveReplicationTestHook{},
		namespace.ID("namespace_id"),
	))
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })
	executor := NewActiveStandbyExecutor(
		currentCluster,
		registry,
		activeExecutor,
		standbyExecutor,
		log.NewNoopLogger(),
		metricsHandler,
		testHooks,
	)

	task := historytasks.NewFakeTask(
		definition.NewWorkflowKey("namespace_id", "workflow_id", "run_id"),
		historytasks.CategoryTransfer,
		time.Time{},
	)
	executable := NewMockExecutable(ctrl)
	executable.EXPECT().GetNamespaceID().Return("namespace_id")
	executable.EXPECT().GetTask().Return(task)
	standbyExecutor.EXPECT().Execute(gomock.Any(), executable).Return(ExecuteResponse{
		ExecutedAsActive: false,
	})

	response := executor.Execute(context.Background(), executable)
	require.False(t, response.ExecutedAsActive)
	require.NoError(t, response.ExecutionErr)
	recordings := capture.Snapshot()[metrics.HistoryPassiveReplicationTestHookCounter.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "ActiveStandbyExecutor", recordings[0].Tags[metrics.OperationTagName])
}
