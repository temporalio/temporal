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

func TestEmitEnvironmentInfo(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config:  WorkerMetricsConfig{},
	}

	nsID := namespace.ID("ns-id")
	nsName := namespace.Name("test-namespace")

	heartbeats := []*workerpb.WorkerHeartbeat{
		{
			WorkerInstanceKey: "go-worker",
			Environment: &workerpb.EnvironmentInfo{
				Runtimes: []*workerpb.EnvironmentInfo_Runtime{
					{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_GO, Version: "1.23.0"},
				},
				Platform: &workerpb.EnvironmentInfo_Platform{
					Variant: &workerpb.EnvironmentInfo_Platform_Linux{
						Linux: &workerpb.EnvironmentInfo_LinuxPlatform{
							Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_AMD64,
						},
					},
				},
			},
		},
		{
			WorkerInstanceKey: "python-worker",
			Environment: &workerpb.EnvironmentInfo{
				Runtimes: []*workerpb.EnvironmentInfo_Runtime{
					{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_CPYTHON, Version: "3.12.0"},
				},
				Platform: &workerpb.EnvironmentInfo_Platform{
					Variant: &workerpb.EnvironmentInfo_Platform_Macos{
						Macos: &workerpb.EnvironmentInfo_MacOSPlatform{
							Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_ARM64,
						},
					},
				},
			},
		},
		{
			// Worker with no environment info — should be skipped
			WorkerInstanceKey: "old-worker",
		},
	}

	emitter.emit(nsID, nsName, heartbeats)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.WorkerEnvironmentRuntimeMetric.Name()]
	require.Len(t, recordings, 2)

	// Check Go worker metric
	require.Equal(t, int64(1), recordings[0].Value)
	require.Equal(t, "test-namespace", recordings[0].Tags["namespace"])
	require.Equal(t, "go", recordings[0].Tags[metrics.WorkerRuntimeTypeTagName])
	require.Equal(t, "linux", recordings[0].Tags[metrics.WorkerOSTagName])
	require.Equal(t, "amd64", recordings[0].Tags[metrics.WorkerArchitectureTagName])

	// Check Python worker metric
	require.Equal(t, int64(1), recordings[1].Value)
	require.Equal(t, "cpython", recordings[1].Tags[metrics.WorkerRuntimeTypeTagName])
	require.Equal(t, "macos", recordings[1].Tags[metrics.WorkerOSTagName])
	require.Equal(t, "arm64", recordings[1].Tags[metrics.WorkerArchitectureTagName])
}

func TestEmitEnvironmentInfoNoPlatform(t *testing.T) {
	captureHandler := metricstest.NewCaptureHandler()
	capture := captureHandler.StartCapture()
	defer captureHandler.StopCapture(capture)

	emitter := &workerMetricsEmitter{
		handler: captureHandler,
		config:  WorkerMetricsConfig{},
	}

	nsID := namespace.ID("ns-id")

	emitter.emit(nsID, namespace.Name("ns"), []*workerpb.WorkerHeartbeat{
		{
			WorkerInstanceKey: "worker-no-platform",
			Environment: &workerpb.EnvironmentInfo{
				Runtimes: []*workerpb.EnvironmentInfo_Runtime{
					{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_JVM},
				},
			},
		},
	})

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.WorkerEnvironmentRuntimeMetric.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, "unknown", recordings[0].Tags[metrics.WorkerOSTagName])
	require.Equal(t, "unknown", recordings[0].Tags[metrics.WorkerArchitectureTagName])
	require.Equal(t, "jvm", recordings[0].Tags[metrics.WorkerRuntimeTypeTagName])
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
