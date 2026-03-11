package workers

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

	// Worker 1: workflow autoscaling enabled, activity disabled
	worker1 := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "worker_1",
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
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	testNamespaceName := namespace.Name("test-namespace")
	emitter.emit(testNamespaceName, []*workerpb.WorkerHeartbeat{worker1, worker2, worker3})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]

	// Counter increments per heartbeat: workflow x3, activity x1, nexus x1 = 5 total recordings
	assert.Len(t, autoscalingMetrics, 5, "expected 5 counter increments")

	pollerTypeCounts := make(map[string]int)
	for _, m := range autoscalingMetrics {
		pollerTypeCounts[m.Tags[metrics.PollerTypeTagName]]++
		assert.Equal(t, string(testNamespaceName), m.Tags["namespace"])
	}
	assert.Equal(t, 3, pollerTypeCounts[metrics.PollerTypeWorkflow], "workflow should have 3 increments")
	assert.Equal(t, 1, pollerTypeCounts[metrics.PollerTypeActivity], "activity should have 1 increment")
	assert.Equal(t, 1, pollerTypeCounts[metrics.PollerTypeNexus], "nexus should have 1 increment")
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
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
	}

	testNamespaceName := namespace.Name("test-namespace")
	emitter.emit(testNamespaceName, []*workerpb.WorkerHeartbeat{worker1})

	snapshot := capture.Snapshot()
	autoscalingMetrics := snapshot[metrics.PollerAutoscalingHeartbeatCount.Name()]
	assert.Empty(t, autoscalingMetrics, "should not record autoscaling metrics when disabled")
}
