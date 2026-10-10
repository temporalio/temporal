package queues

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func newTestReaderGroup(monitor Monitor, slices ...Slice) *ReaderGroup {
	rg := NewReaderGroup(func(readerID int64, s []Slice) Reader {
		return NewReader(
			readerID,
			s,
			&ReaderOptions{
				BatchSize:            dynamicconfig.GetIntPropertyFn(10),
				MaxPendingTasksCount: dynamicconfig.GetIntPropertyFn(100),
				PollBackoffInterval:  dynamicconfig.GetDurationPropertyFn(200 * time.Millisecond),
				MaxPredicateSize:     dynamicconfig.GetIntPropertyFn(10),
			},
			nil,
			nil,
			clock.NewRealTimeSource(),
			NewReaderPriorityRateLimiter(func() float64 { return 20 }, 1),
			monitor,
			NoopReaderCompletionFn,
			log.NewTestLogger(),
			metrics.NoopMetricsHandler,
		)
	})
	if len(slices) > 0 {
		rg.NewReader(DefaultReaderId, slices...)
	}
	return rg
}

func TestMoveGroupAction_EmitsPendingTasksMetric(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockNamespaceRegistry.EXPECT().GetNamespaceName(namespace.ID("ns-1-id")).Return(namespace.Name("ns-1-name"), nil).AnyTimes()
	mockNamespaceRegistry.EXPECT().GetNamespaceName(namespace.ID("ns-2-id")).Return(namespace.Name("ns-2-name"), nil).AnyTimes()

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	action := newMoveGroupAction(
		3,
		GrouperNamespaceID{},
		100, // threshold higher than pending counts so no move happens
		3.0,
		tasks.CategoryTransfer.Name(),
		mockNamespaceRegistry,
		captureHandler,
		log.NewTestLogger(),
	)

	monitor := newMonitor(tasks.CategoryTypeImmediate, clock.NewRealTimeSource(), log.NewTestLogger(), metrics.NoopMetricsHandler, &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		ReaderStuckShadowMode:       dynamicconfig.GetBoolPropertyFn(false),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})

	scope := NewScope(
		NewRange(tasks.NewKey(time.Unix(0, 0), 1), tasks.NewKey(time.Unix(0, 0), 100)),
		predicates.Universal[tasks.Task](),
	)
	slice := NewSlice(nil, nil, monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler)
	slice.iterators = nil

	// Add 15 tasks for ns-1 and 25 tasks for ns-2
	for i := 1; i <= 15; i++ {
		exec := newMockExecutableForTest(controller, tasks.NewKey(time.Unix(0, 0), int64(i)), "ns-1-id")
		slice.add(exec)
	}
	for i := 16; i <= 40; i++ {
		exec := newMockExecutableForTest(controller, tasks.NewKey(time.Unix(0, 0), int64(i)), "ns-2-id")
		slice.add(exec)
	}

	readerGroup := newTestReaderGroup(monitor, slice)

	moved := action.Run(readerGroup)
	require.False(t, moved)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.QueuePendingTasksPerNamespace.Name()]
	require.Len(t, recordings, 2)

	recordingsByNS := make(map[string]int64)
	for _, r := range recordings {
		require.Equal(t, tasks.CategoryTransfer.Name(), r.Tags["task_category"])
		recordingsByNS[r.Tags["namespace"]] = r.Value.(int64)
	}

	require.Equal(t, int64(15), recordingsByNS["ns-1-name"])
	require.Equal(t, int64(25), recordingsByNS["ns-2-name"])
}

func TestMoveGroupAction_ZeroPendingTasks_DoesNotEmit(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	action := newMoveGroupAction(
		3,
		GrouperNamespaceID{},
		100,
		3.0,
		tasks.CategoryTransfer.Name(),
		nil,
		captureHandler,
		log.NewTestLogger(),
	)

	monitor := newMonitor(tasks.CategoryTypeImmediate, clock.NewRealTimeSource(), log.NewTestLogger(), metrics.NoopMetricsHandler, &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		ReaderStuckShadowMode:       dynamicconfig.GetBoolPropertyFn(false),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})

	scope := NewScope(
		NewRange(tasks.NewKey(time.Unix(0, 0), 1), tasks.NewKey(time.Unix(0, 0), 100)),
		predicates.Universal[tasks.Task](),
	)
	slice := NewSlice(nil, nil, monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler)
	slice.iterators = nil

	readerGroup := newTestReaderGroup(monitor, slice)

	moved := action.Run(readerGroup)
	require.False(t, moved)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.QueuePendingTasksPerNamespace.Name()]
	require.Empty(t, recordings)
}

func TestMoveGroupAction_NamespaceResolutionFallback(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	// Return error when looking up namespace to test fallback to raw namespace ID
	mockNamespaceRegistry.EXPECT().GetNamespaceName(namespace.ID("unregistered-id")).Return(namespace.EmptyName, errors.New("not found")).AnyTimes()

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	action := newMoveGroupAction(
		3,
		GrouperNamespaceID{},
		100,
		3.0,
		tasks.CategoryTimer.Name(),
		mockNamespaceRegistry,
		captureHandler,
		log.NewTestLogger(),
	)

	monitor := newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), log.NewTestLogger(), metrics.NoopMetricsHandler, &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		ReaderStuckShadowMode:       dynamicconfig.GetBoolPropertyFn(false),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})

	scope := NewScope(
		NewRange(tasks.NewKey(time.Unix(0, 0), 1), tasks.NewKey(time.Unix(0, 0), 100)),
		predicates.Universal[tasks.Task](),
	)
	slice := NewSlice(nil, nil, monitor, scope, GrouperNamespaceID{}, noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler)
	slice.iterators = nil

	exec := newMockExecutableForTest(controller, tasks.NewKey(time.Unix(0, 0), 1), "unregistered-id")
	slice.add(exec)

	readerGroup := newTestReaderGroup(monitor, slice)

	moved := action.Run(readerGroup)
	require.False(t, moved)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.QueuePendingTasksPerNamespace.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "unregistered-id", recordings[0].Tags["namespace"])
	require.Equal(t, tasks.CategoryTimer.Name(), recordings[0].Tags["task_category"])
	require.Equal(t, int64(1), recordings[0].Value.(int64))
}

func TestMoveGroupAction_OutboundGrouperKeySupport(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	mockNamespaceRegistry := namespace.NewMockRegistry(controller)
	mockNamespaceRegistry.EXPECT().GetNamespaceName(namespace.ID("outbound-ns-id")).Return(namespace.Name("outbound-ns-name"), nil).AnyTimes()

	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	action := newMoveGroupAction(
		3,
		GrouperStateMachineNamespaceIDAndDestination{},
		100,
		3.0,
		tasks.CategoryOutbound.Name(),
		mockNamespaceRegistry,
		captureHandler,
		log.NewTestLogger(),
	)

	monitor := newMonitor(tasks.CategoryTypeImmediate, clock.NewRealTimeSource(), log.NewTestLogger(), metrics.NoopMetricsHandler, &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		ReaderStuckShadowMode:       dynamicconfig.GetBoolPropertyFn(false),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})

	scope := NewScope(
		NewRange(tasks.NewKey(time.Unix(0, 0), 1), tasks.NewKey(time.Unix(0, 0), 100)),
		predicates.Universal[tasks.Task](),
	)
	slice := NewSlice(nil, nil, monitor, scope, GrouperStateMachineNamespaceIDAndDestination{}, noPredicateSizeLimit, defaultMaxPendingKeys, metrics.NoopMetricsHandler)
	slice.iterators = nil

	// Outbound task with TaskGroupNamespaceIDAndDestination key
	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetKey().Return(tasks.NewKey(time.Unix(0, 0), 1)).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return("outbound-ns-id").AnyTimes()

	exec := NewMockExecutable(controller)
	exec.EXPECT().GetKey().Return(tasks.NewKey(time.Unix(0, 0), 1)).AnyTimes()
	exec.EXPECT().GetTask().Return(mockTask).AnyTimes()
	exec.EXPECT().GetNamespaceID().Return("outbound-ns-id").AnyTimes()
	exec.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()

	slice.add(exec)

	readerGroup := newTestReaderGroup(monitor, slice)

	moved := action.Run(readerGroup)
	require.False(t, moved)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.QueuePendingTasksPerNamespace.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "outbound-ns-name", recordings[0].Tags["namespace"])
	require.Equal(t, tasks.CategoryOutbound.Name(), recordings[0].Tags["task_category"])
	require.Equal(t, int64(1), recordings[0].Value.(int64))
}

func newMockExecutableForTest(controller *gomock.Controller, key tasks.Key, namespaceID string) *MockExecutable {
	mockTask := tasks.NewMockTask(controller)
	mockTask.EXPECT().GetKey().Return(key).AnyTimes()
	mockTask.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()

	exec := NewMockExecutable(controller)
	exec.EXPECT().GetKey().Return(key).AnyTimes()
	exec.EXPECT().GetTask().Return(mockTask).AnyTimes()
	exec.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
	exec.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
	return exec
}
