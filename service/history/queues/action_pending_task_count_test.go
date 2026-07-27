package queues

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/predicates"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestQueuePendingTaskActionRunSplitsAndClearsSelectedTaskGroup(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	// Build real slices with three pending task groups. Run should unload only
	// the largest group from the first slice and leave the other groups pending.
	keyToUnload := "namespace-id"
	keyToKeep := "other-namespace-id"
	untouchedKey := "untouched-namespace-id"
	scopeToSplit := NewScope(
		NewRange(
			tasks.NewKey(time.Unix(0, 0), 1),
			tasks.NewKey(time.Unix(0, 0), 100),
		),
		predicates.Universal[tasks.Task](),
	)
	monitor := newMonitor(tasks.CategoryTypeScheduled, clock.NewRealTimeSource(), &MonitorOptions{
		PendingTasksCriticalCount:   dynamicconfig.GetIntPropertyFn(1000),
		ReaderStuckCriticalAttempts: dynamicconfig.GetIntPropertyFn(5),
		SliceCountCriticalThreshold: dynamicconfig.GetIntPropertyFn(50),
	})
	sliceToSplit := NewSlice(
		nil,
		nil,
		monitor,
		scopeToSplit,
		GrouperNamespaceID{},
		noPredicateSizeLimit,
		defaultMaxPendingKeys,
		metrics.NoopMetricsHandler,
	)
	sliceToSplit.iterators = nil

	untouchedSlice := NewSlice(
		nil,
		nil,
		monitor,
		NewScope(
			NewRange(
				tasks.NewKey(time.Unix(0, 0), 100),
				tasks.NewKey(time.Unix(0, 0), 200),
			),
			predicates.Universal[tasks.Task](),
		),
		GrouperNamespaceID{},
		noPredicateSizeLimit,
		defaultMaxPendingKeys,
		metrics.NoopMetricsHandler,
	)
	untouchedSlice.iterators = nil

	var tasksToUnload []Executable
	var tasksToKeep []Executable
	addExecutables := func(slice *SliceImpl, namespaceID string, count int, startTaskID int64, expectCancel bool) {
		for i := range count {
			executable := NewMockExecutable(controller)
			executable.EXPECT().GetKey().Return(tasks.NewKey(time.Unix(0, 0), startTaskID+int64(i))).AnyTimes()
			executable.EXPECT().GetNamespaceID().Return(namespaceID).AnyTimes()
			executable.EXPECT().State().Return(ctasks.TaskStatePending).AnyTimes()
			if expectCancel {
				executable.EXPECT().Cancel().Times(1)
			}
			if namespaceID == keyToUnload {
				tasksToUnload = append(tasksToUnload, executable)
			}
			if namespaceID == keyToKeep {
				tasksToKeep = append(tasksToKeep, executable)
			}
			slice.add(executable)
		}
	}
	addExecutables(sliceToSplit, keyToUnload, 10, 1, true)
	addExecutables(sliceToSplit, keyToKeep, 5, 11, false)
	addExecutables(untouchedSlice, untouchedKey, 2, 100, false)
	monitor.SetSlicePendingTaskCount(sliceToSplit, 15)
	monitor.SetSlicePendingTaskCount(untouchedSlice, 2)

	// Use a real reader group so the test exercises the same reader locking and
	// split/clear path used by queue pending task mitigation.
	readerGroup := NewReaderGroup(func(readerID int64, slices []Slice) Reader {
		return NewReader(
			readerID,
			slices,
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
	readerGroup.NewReader(DefaultReaderId, sliceToSplit, untouchedSlice)
	defer readerGroup.ForEach(func(_ int64, reader Reader) {
		readerImpl := reader.(*ReaderImpl)
		readerImpl.Lock()
		defer readerImpl.Unlock()
		if readerImpl.throttleTimer != nil {
			readerImpl.throttleTimer.Stop()
			readerImpl.throttleTimer = nil
		}
	})

	// Configure Run to reduce pending tasks below 80% of the critical count.
	// The largest task group is sufficient, so only that group should be moved.
	action := newQueuePendingTaskAction(
		&AlertAttributesQueuePendingTaskCount{
			CurrentPendingTaskCount:   17,
			CiriticalPendingTaskCount: 11,
		},
		monitor,
		2,
		GrouperNamespaceID{},
	)

	require.True(t, action.Run(readerGroup))

	// The action keeps per-slice stats after gathering. They must remain a snapshot
	// even though Run subsequently splits/clears the source slice's task tracker.
	require.Equal(t, 10, action.pendingTasksPerKeyPerSlice[sliceToSplit][keyToUnload])
	require.Equal(t, 5, action.pendingTasksPerKeyPerSlice[sliceToSplit][keyToKeep])
	require.Equal(t, 2, action.pendingTasksPerKeyPerSlice[untouchedSlice][untouchedKey])

	reader, ok := readerGroup.ReaderByID(DefaultReaderId)
	require.True(t, ok)
	nextReader, ok := readerGroup.ReaderByID(DefaultReaderId + 1)
	require.True(t, ok)

	var readerSlices []Slice
	reader.WalkSlices(func(slice Slice) {
		readerSlices = append(readerSlices, slice)
	})
	require.Len(t, readerSlices, 2)
	require.Equal(t, map[any]int{keyToKeep: 5}, readerSlices[0].TaskStats().PendingPerKey)
	require.Equal(t, map[any]int{untouchedKey: 2}, readerSlices[1].TaskStats().PendingPerKey)

	var movedSlices []Slice
	nextReader.WalkSlices(func(slice Slice) {
		movedSlices = append(movedSlices, slice)
	})
	require.Len(t, movedSlices, 1)
	require.Empty(t, movedSlices[0].TaskStats().PendingPerKey)
	movedScope := movedSlices[0].Scope()
	for _, task := range tasksToUnload {
		require.True(t, movedScope.Contains(task))
	}
	for _, task := range tasksToKeep {
		require.False(t, movedScope.Contains(task))
	}
	require.Equal(t, 7, monitor.GetTotalPendingTaskCount())
}
