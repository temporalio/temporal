package workers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
)

type captureEventLogger struct {
	embedded.Logger
	records []otellog.Record
}

func (c *captureEventLogger) Emit(_ context.Context, r otellog.Record) {
	c.records = append(c.records, r)
}
func (c *captureEventLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

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

func TestRuntimeTypeNameUnknownValue(t *testing.T) {
	require.Equal(t, "go", runtimeTypeName(workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_GO))
	require.Equal(t, "unknown", runtimeTypeName(workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_UNSPECIFIED))
	require.Equal(t, "unknown", runtimeTypeName(workerpb.EnvironmentInfo_Runtime_RuntimeType(999)))
}

func TestArchitectureNameUnknownValue(t *testing.T) {
	require.Equal(t, "amd64", architectureName(workerpb.EnvironmentInfo_ARCHITECTURE_AMD64))
	require.Equal(t, "unknown", architectureName(workerpb.EnvironmentInfo_ARCHITECTURE_UNSPECIFIED))
	require.Equal(t, "unknown", architectureName(workerpb.EnvironmentInfo_Architecture(999)))
}

func TestEmitWorkerConfigEvent(t *testing.T) {
	eventLogger := &captureEventLogger{}

	hb := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "w1",
		TaskQueue:         "my-queue",
		SdkName:           "temporal-go",
		Environment: &workerpb.EnvironmentInfo{
			Runtimes: []*workerpb.EnvironmentInfo_Runtime{
				{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_GO},
				{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_ROADRUNNER},
			},
			HostingEnvironments: []*workerpb.EnvironmentInfo_HostingEnvironment{
				{Type: workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_DOCKER},
				{Type: workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_K8S},
			},
			Platform: &workerpb.EnvironmentInfo_Platform{
				Variant: &workerpb.EnvironmentInfo_Platform_Linux{
					Linux: &workerpb.EnvironmentInfo_LinuxPlatform{
						Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_ARM64,
					},
				},
			},
		},
	}

	emitWorkerConfigEvent(eventLogger, namespace.Name("ns"), hb)

	require.Len(t, eventLogger.records, 1)
	require.Equal(t, "worker_config", eventLogger.records[0].EventName())

	attrs := map[string]otellog.Value{}
	eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	require.Equal(t, "temporal-go", attrs["sdk_name"].AsString())
	require.Equal(t, "go,roadrunner", attrs["runtimes"].AsString())
	require.Equal(t, "docker,k8s", attrs["hosting_environments"].AsString())
	require.Equal(t, "linux", attrs["os"].AsString())
	require.Equal(t, "arm64", attrs["architecture"].AsString())
}
