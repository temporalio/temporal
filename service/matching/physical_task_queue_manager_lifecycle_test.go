package matching

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
)

func TestPhysicalTaskQueueManagerStopBeforeStart(t *testing.T) {
	manager, capture, events := newLifecycleTestPhysicalTaskQueueManager(t)

	// A partition can publish its physical queue before starting it, allowing an
	// unload to stop the queue while it is still initialized.
	manager.Stop(unloadCauseInitError)
	manager.Stop(unloadCauseConflict)
	manager.Start()
	manager.Start()

	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&manager.status))
	require.ErrorIs(t, manager.tqCtx.Err(), context.Canceled)
	require.Empty(t, drainLifecycleEvents(events))
	require.Empty(t, capture.SnapshotMetric(metrics.TaskQueueStartedCounter.Name()))
	require.Empty(t, capture.SnapshotMetric(metrics.TaskQueueStoppedCounter.Name()))
	require.Empty(t, capture.SnapshotMetric(metrics.LoadedPhysicalTaskQueueGauge.Name()))
}

func TestPhysicalTaskQueueManagerRepeatedStartStop(t *testing.T) {
	manager, capture, events := newLifecycleTestPhysicalTaskQueueManager(t)

	manager.Start()
	manager.Start()
	require.NoError(t, manager.tqCtx.Err())
	manager.Stop(unloadCauseIdle)
	manager.Stop(unloadCauseConflict)
	manager.Start()

	require.Equal(t, common.DaemonStatusStopped, atomic.LoadInt32(&manager.status))
	require.ErrorIs(t, manager.tqCtx.Err(), context.Canceled)
	require.Equal(t, []string{"backlog start", "matcher start", "backlog stop", "matcher stop"}, drainLifecycleEvents(events))
	require.Len(t, capture.SnapshotMetric(metrics.TaskQueueStartedCounter.Name()), 1)
	require.Len(t, capture.SnapshotMetric(metrics.TaskQueueStoppedCounter.Name()), 1)
	gauges := capture.SnapshotMetric(metrics.LoadedPhysicalTaskQueueGauge.Name())
	require.Len(t, gauges, 2)
	require.InDelta(t, 1, gauges[0].Value, 0)
	require.InDelta(t, 0, gauges[1].Value, 0)
}

func TestPhysicalTaskQueueManagerStopWaitsForStart(t *testing.T) {
	manager, _, events := newLifecycleTestPhysicalTaskQueueManager(t)
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	startDone := make(chan struct{})
	stopDone := make(chan struct{})
	matcher := manager.matcher.(*lifecycleTestMatcher)
	originalStart := matcher.start
	matcher.start = func() {
		close(startEntered)
		<-releaseStart
		originalStart()
	}

	go func() {
		manager.Start()
		close(startDone)
	}()
	<-startEntered
	go func() {
		manager.Stop(unloadCauseConflict)
		close(stopDone)
	}()

	// Keep startup in progress while Stop has an opportunity to run. Mutex
	// waits are not durably blocking in synctest, so this uses a bounded wait.
	stoppedDuringStart := false
	select {
	case <-stopDone:
		stoppedDuringStart = true
	case <-time.After(50 * time.Millisecond):
	}
	close(releaseStart)
	for _, done := range []<-chan struct{}{startDone, stopDone} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("manager lifecycle did not complete")
		}
	}

	require.False(t, stoppedDuringStart, "Stop must not tear down a manager whose components are still starting")
	require.ErrorIs(t, manager.tqCtx.Err(), context.Canceled)
	require.Equal(t, []string{"backlog start", "matcher start", "backlog stop", "matcher stop"}, drainLifecycleEvents(events))
}

type lifecycleTestBacklogManager struct {
	backlogManager
	start func()
	stop  func()
}

func (m *lifecycleTestBacklogManager) Start() { m.start() }
func (m *lifecycleTestBacklogManager) Stop()  { m.stop() }

type lifecycleTestMatcher struct {
	matcherInterface
	start func()
	stop  func()
}

func (m *lifecycleTestMatcher) Start() { m.start() }
func (m *lifecycleTestMatcher) Stop()  { m.stop() }

func newLifecycleTestPhysicalTaskQueueManager(t *testing.T) (*physicalTaskQueueManagerImpl, *metricstest.Capture, <-chan string) {
	t.Helper()
	controller := gomock.NewController(t)
	logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	ns, registry := createMockNamespaceCache(controller, namespace.Name("lifecycle-test"))
	engine := createTestMatchingEngine(logger, controller, defaultTestConfig(), nil, registry)
	handler := metricstest.NewCaptureHandler()
	engine.metricsHandler = handler
	capture := handler.StartCapture()
	t.Cleanup(func() { handler.StopCapture(capture) })
	queue := defaultTqId()
	partition := queue.Partition()
	config := newTaskQueueConfig(partition.TaskQueue(), engine.config, ns.Name())
	userData := newUserDataManager(engine.taskManager, engine.matchingRawClient, nil, nil, nil, partition, config, logger, registry)
	partitionManager, err := newTaskQueuePartitionManager(engine, ns, partition, config, logger, logger, handler, userData)
	require.NoError(t, err)
	manager, err := newPhysicalTaskQueueManager(partitionManager, queue)
	require.NoError(t, err)
	partitionManager.defaultQueueFuture.Set(manager, nil)
	t.Cleanup(manager.tqCtxCancel)

	events := make(chan string, 16)
	manager.backlogMgr = &lifecycleTestBacklogManager{
		backlogManager: manager.backlogMgr,
		start:          func() { events <- "backlog start" },
		stop: func() {
			if manager.tqCtx.Err() != nil {
				events <- "context canceled before backlog stop"
			}
			events <- "backlog stop"
		},
	}
	manager.matcher = &lifecycleTestMatcher{
		matcherInterface: manager.matcher,
		start:            func() { events <- "matcher start" },
		stop:             func() { events <- "matcher stop" },
	}
	manager.liveness = newLiveness(clock.NewEventTimeSource(), func() time.Duration { return time.Hour }, func() {})
	return manager, capture, events
}

func drainLifecycleEvents(events <-chan string) []string {
	var result []string
	for {
		select {
		case event := <-events:
			result = append(result, event)
		default:
			return result
		}
	}
}
