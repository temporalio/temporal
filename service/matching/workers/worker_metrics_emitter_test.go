package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
)

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
