package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
)

func TestWorkersPerProcessMetric(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config:  WorkerMetricsConfig{},
	}

	nsID := namespace.ID("ns-id")
	nsName := namespace.Name("ns-name")

	emitter.emit(nsID, nsName, []*workerpb.WorkerHeartbeat{
		{WorkerInstanceKey: "w1"},
		{WorkerInstanceKey: "w2"},
		{WorkerInstanceKey: "w3"},
	})

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.WorkerRegistryWorkersPerProcess.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(3), recordings[0].Value)
}

func TestPollerAutoscalingMetrics(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePluginMetrics:            dynamicconfig.GetBoolPropertyFn(false),
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(true),
		},
	}

	testNamespaceID := namespace.ID("test-namespace-id")
	testNamespaceName := namespace.Name("test-namespace")
	testTaskQueue := "test-task-queue"

	// Worker 1: workflow autoscaling enabled, activity disabled
	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		ActivityPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: false,
		},
	}

	// Worker 2: workflow, activity, and nexus all enabled
	worker2 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_2",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		ActivityPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		NexusPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	// Worker 3: workflow only
	worker3 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_3",
		TaskQueue:         testTaskQueue,
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	emitter.emit(testNamespaceID, testNamespaceName, []*workerpb.WorkerHeartbeat{worker1, worker2, worker3})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]

	// Counter increments per heartbeat: workflow x3, activity x1, nexus x1 = 5 total recordings
	assert.Len(t, autoscalingMetrics, 5, "expected 5 counter increments")

	taskTypeCounts := make(map[string]int)
	for _, m := range autoscalingMetrics {
		taskTypeCounts[m.Tags[metrics.TaskTypeTagName]]++
		assert.Equal(t, string(testNamespaceName), m.Tags["namespace"])
		assert.Equal(t, "__omitted__", m.Tags["taskqueue"])
	}
	assert.Equal(t, 3, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_WORKFLOW.String()], "workflow should have 3 increments")
	assert.Equal(t, 1, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_ACTIVITY.String()], "activity should have 1 increment")
	assert.Equal(t, 1, taskTypeCounts[enumspb.TASK_QUEUE_TYPE_NEXUS.String()], "nexus should have 1 increment")
}

func TestPollerAutoscalingMetricsDisabled(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config: WorkerMetricsConfig{
			EnablePluginMetrics:            dynamicconfig.GetBoolPropertyFn(false),
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(false),
		},
	}

	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
		TaskQueue:         "test-task-queue",
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	testNamespaceID := namespace.ID("test-namespace-id")
	testNamespaceName := namespace.Name("test-namespace")
	emitter.emit(testNamespaceID, testNamespaceName, []*workerpb.WorkerHeartbeat{worker1})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]
	assert.Empty(t, autoscalingMetrics, "should not record autoscaling metrics when disabled")
}

func perWorkerEmitter(handler metrics.Handler, enabled bool) *workerMetricsEmitter {
	return &workerMetricsEmitter{
		handler: handler,
		config: WorkerMetricsConfig{
			EnablePollerAutoscalingMetrics: dynamicconfig.GetBoolPropertyFn(true),
			EnablePerWorkerPollerMetrics:   dynamicconfig.GetBoolPropertyFnFilteredByNamespace(enabled),
		},
	}
}

func TestPerWorkerPollerMetrics(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	// The SDK fills all four poller blocks regardless of which sub-workers exist, so a zero
	// target is how "no autoscaler for this poller" arrives. Here nexus is not run at all and
	// the activity pollers have a fixed count; neither should produce a series.
	worker := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey:        "worker-1",
		TaskQueue:                "test-task-queue",
		WorkflowPollerInfo:       &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 10},
		WorkflowStickyPollerInfo: &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 12},
		ActivityPollerInfo:       &workerpb.WorkerPollerInfo{TargetPollers: 0},
		NexusPollerInfo:          &workerpb.WorkerPollerInfo{},
	}

	perWorkerEmitter(captureHandler, true).
		emit(namespace.ID("ns-id"), namespace.Name("ns"), []*workerpb.WorkerHeartbeat{worker})

	recordings := capture.Snapshot()[metrics.WorkerPollerTarget.Name()]
	require.Len(t, recordings, 2, "only the two auto-scaled workflow pollers")

	byKind := make(map[string]float64)
	for _, m := range recordings {
		require.Equal(t, "worker-1", m.Tags[metrics.WorkerInstanceKeyTagName])
		require.Equal(t, enumspb.TASK_QUEUE_TYPE_WORKFLOW.String(), m.Tags[metrics.TaskTypeTagName])
		byKind[m.Tags[metrics.PollerKindTagName]] = m.Value.(float64)
	}
	require.InDelta(t, float64(10), byKind[pollerKindNormal], 0)
	require.InDelta(t, float64(12), byKind[pollerKindSticky], 0)
}

func TestPerWorkerPollerMetricsGating(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	newWorker := func(taskQueue string) *workerpb.WorkerHeartbeat {
		return &workerpb.WorkerHeartbeat{
			WorkerInstanceKey:  "worker-1",
			TaskQueue:          taskQueue,
			ActivityPollerInfo: &workerpb.WorkerPollerInfo{IsAutoscaling: true, TargetPollers: 3},
		}
	}
	// primitives.internalTaskQueuePrefix, unexported.
	systemWorker := newWorker("temporal-sys-scanner-tq")

	perWorkerEmitter(captureHandler, false).
		emit(namespace.ID("ns-id"), namespace.Name("ns"), []*workerpb.WorkerHeartbeat{newWorker("tq")})
	require.Empty(t, capture.Snapshot()[metrics.WorkerPollerTarget.Name()], "disabled for the namespace")
	require.Len(t, capture.Snapshot()[metrics.PollerAutoscalingHeartbeatCount.Name()], 1,
		"the autoscaling adoption counter is gated independently")

	// Temporal's own internal workers heartbeat too, and are excluded to match ListWorkers.
	perWorkerEmitter(captureHandler, true).
		emit(namespace.ID("ns-id"), namespace.Name("ns"), []*workerpb.WorkerHeartbeat{systemWorker})
	require.Empty(t, capture.Snapshot()[metrics.WorkerPollerTarget.Name()], "system worker excluded")
}
