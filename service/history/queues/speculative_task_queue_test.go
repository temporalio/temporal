package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestSpeculativeTaskQueue_RoutesTasksToTheirSchedulers(t *testing.T) {
	ctrl := gomock.NewController(t)
	timeSource := clock.NewEventTimeSource()
	timeSource.Update(time.Now().UTC())

	clusterMetadata := cluster.NewMockMetadata(ctrl)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	namespaceRegistry.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.LocalNamespaceEntry, nil).AnyTimes()

	newScheduler := func(submitted chan<- ctasks.Task) *ctasks.MockScheduler[ctasks.Task] {
		scheduler := ctasks.NewMockScheduler[ctasks.Task](ctrl)
		scheduler.EXPECT().TrySubmit(gomock.Any()).DoAndReturn(func(task ctasks.Task) bool {
			submitted <- task
			return true
		}).Times(1)
		return scheduler
	}
	workflowTaskTimeoutSubmitted := make(chan ctasks.Task, 1)
	chasmWakeupSubmitted := make(chan ctasks.Task, 1)

	queue := NewSpeculativeTaskQueue(
		newScheduler(workflowTaskTimeoutSubmitted),
		newScheduler(chasmWakeupSubmitted),
		NewNoopPriorityAssigner(),
		nil,
		namespaceRegistry,
		clusterMetadata,
		timeSource,
		chasm.NewRegistry(log.NewTestLogger()),
		metrics.NoopMetricsHandler,
		telemetry.NoopTracer,
		log.NewTestLogger(),
	)
	queue.Start()
	defer queue.Stop()

	workflowTaskTimeout := &tasks.WorkflowTaskTimeoutTask{
		WorkflowKey:         tests.WorkflowKey,
		VisibilityTimestamp: timeSource.Now(),
		InMemory:            true,
	}
	wakeup := &tasks.ChasmMemoryWakeupTask{
		WorkflowKey:         tests.WorkflowKey,
		VisibilityTimestamp: timeSource.Now(),
	}
	queue.NotifyNewTasks([]tasks.Task{workflowTaskTimeout, wakeup})

	select {
	case task := <-workflowTaskTimeoutSubmitted:
		executable, ok := task.(*speculativeWorkflowTaskTimeoutExecutable)
		require.True(t, ok)
		require.Same(t, workflowTaskTimeout, executable.GetTask())
	case <-time.After(5 * time.Second):
		require.FailNow(t, "workflow task timeout was not submitted")
	}

	select {
	case task := <-chasmWakeupSubmitted:
		executable, ok := task.(*chasmMemoryWakeupExecutable)
		require.True(t, ok)
		require.Same(t, wakeup, executable.GetTask())

		wakeup.Cancel()
		require.Equal(t, ctasks.TaskStateCancelled, executable.State())
	case <-time.After(5 * time.Second):
		require.FailNow(t, "wakeup task was not submitted")
	}
}
