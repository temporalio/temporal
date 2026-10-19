package workers

import (
	"context"
	"testing"
	"time"

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
	"google.golang.org/protobuf/types/known/timestamppb"
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
		WorkerIdentity:    "12345@my-host",
		TaskQueue:         "my-queue",
		SdkName:           "temporal-go",
		SdkVersion:        "1.30.0",
		StartTime:         timestamppb.New(time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC)),
		Plugins:           []*workerpb.PluginInfo{{Name: "my-plugin", Version: "1.0.0"}},
		Drivers:           []*workerpb.StorageDriverInfo{{Type: "s3"}},
		WorkflowPollerInfo: &workerpb.WorkerPollerInfo{
			IsAutoscaling: true,
		},
		Environment: &workerpb.EnvironmentInfo{
			Runtimes: []*workerpb.EnvironmentInfo_Runtime{
				{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_GO, Version: "go1.23.0"},
				{Type: workerpb.EnvironmentInfo_Runtime_RUNTIME_TYPE_ROADRUNNER, Version: "v2024.3"},
			},
			HostingEnvironments: []*workerpb.EnvironmentInfo_HostingEnvironment{
				{Type: workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_DOCKER},
				{Type: workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_K8S},
			},
			Platform: &workerpb.EnvironmentInfo_Platform{
				Variant: &workerpb.EnvironmentInfo_Platform_Linux{
					Linux: &workerpb.EnvironmentInfo_LinuxPlatform{
						Version:      "6.1.0",
						Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_ARM64,
						Libc:         workerpb.EnvironmentInfo_LinuxPlatform_LIBC_MUSL,
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
	require.Equal(t, "ns", attrs["namespace"].AsString())
	require.Equal(t, "my-queue", attrs["task_queue"].AsString())
	require.Equal(t, "w1", attrs["worker_instance_key"].AsString())
	require.Equal(t, "12345@my-host", attrs["worker_identity"].AsString())
	require.Equal(t, "2026-10-09T12:00:00Z", attrs["start_time"].AsString())
	require.Equal(t, "temporal-go", attrs["sdk_name"].AsString())
	require.Equal(t, "1.30.0", attrs["sdk_version"].AsString())
	require.Equal(t, "linux", attrs["os"].AsString())
	require.Equal(t, "6.1.0", attrs["os_version"].AsString())
	require.Equal(t, "arm64", attrs["architecture"].AsString())
	require.Equal(t, "musl", attrs["libc"].AsString())
	require.True(t, attrs["workflow_poller_autoscaling"].AsBool())
	require.False(t, attrs["activity_poller_autoscaling"].AsBool())
	require.False(t, attrs["nexus_poller_autoscaling"].AsBool())

	// Runtimes: structured array with type and version
	runtimes := attrs["runtimes"].AsSlice()
	require.Len(t, runtimes, 2)
	rt0 := mapValueToMap(runtimes[0])
	require.Equal(t, "go", rt0["type"].AsString())
	require.Equal(t, "go1.23.0", rt0["version"].AsString())
	rt1 := mapValueToMap(runtimes[1])
	require.Equal(t, "roadrunner", rt1["type"].AsString())
	require.Equal(t, "v2024.3", rt1["version"].AsString())

	// Hosting environments: structured array
	hostingEnvs := attrs["hosting_environments"].AsSlice()
	require.Len(t, hostingEnvs, 2)
	require.Equal(t, "docker", mapValueToMap(hostingEnvs[0])["type"].AsString())
	require.Equal(t, "k8s", mapValueToMap(hostingEnvs[1])["type"].AsString())

	// Plugins: structured array with name and version
	plugins := attrs["plugins"].AsSlice()
	require.Len(t, plugins, 1)
	p0 := mapValueToMap(plugins[0])
	require.Equal(t, "my-plugin", p0["name"].AsString())
	require.Equal(t, "1.0.0", p0["version"].AsString())

	// Drivers: string array
	drivers := attrs["drivers"].AsSlice()
	require.Len(t, drivers, 1)
	require.Equal(t, "s3", drivers[0].AsString())
}

// mapValueToMap converts an OTEL MapValue to a map for test assertions.
func mapValueToMap(v otellog.Value) map[string]otellog.Value {
	m := map[string]otellog.Value{}
	for _, kv := range v.AsMap() {
		m[kv.Key] = kv.Value
	}
	return m
}

func TestEmitWorkerConfigEventNoEnvironment(t *testing.T) {
	eventLogger := &captureEventLogger{}

	hb := &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "w1",
		SdkName:           "temporal-go",
	}

	emitWorkerConfigEvent(eventLogger, namespace.Name("ns"), hb)

	attrs := map[string]otellog.Value{}
	eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	// Without environment, os and architecture should be "unknown" (not empty)
	require.Equal(t, "unknown", attrs["os"].AsString())
	require.Equal(t, "unknown", attrs["architecture"].AsString())
}

func TestEmitWorkerConfigEventMacOS(t *testing.T) {
	eventLogger := &captureEventLogger{}

	emitWorkerConfigEvent(eventLogger, namespace.Name("ns"), &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "w1",
		Environment: &workerpb.EnvironmentInfo{
			Platform: &workerpb.EnvironmentInfo_Platform{
				Variant: &workerpb.EnvironmentInfo_Platform_Macos{
					Macos: &workerpb.EnvironmentInfo_MacOSPlatform{
						Version:      "14.2",
						Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_ARM64,
					},
				},
			},
		},
	})

	attrs := map[string]otellog.Value{}
	eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	require.Equal(t, "macos", attrs["os"].AsString())
	require.Equal(t, "14.2", attrs["os_version"].AsString())
	require.Equal(t, "arm64", attrs["architecture"].AsString())
	require.Empty(t, attrs["libc"].AsString())
}

func TestEmitWorkerConfigEventWindows(t *testing.T) {
	eventLogger := &captureEventLogger{}

	emitWorkerConfigEvent(eventLogger, namespace.Name("ns"), &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "w1",
		Environment: &workerpb.EnvironmentInfo{
			Platform: &workerpb.EnvironmentInfo_Platform{
				Variant: &workerpb.EnvironmentInfo_Platform_Windows{
					Windows: &workerpb.EnvironmentInfo_WindowsPlatform{
						Version:      "10.0.19045",
						Architecture: workerpb.EnvironmentInfo_ARCHITECTURE_AMD64,
						Crt:          workerpb.EnvironmentInfo_WindowsPlatform_CRT_UCRT,
					},
				},
			},
		},
	})

	attrs := map[string]otellog.Value{}
	eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	require.Equal(t, "windows", attrs["os"].AsString())
	require.Equal(t, "10.0.19045", attrs["os_version"].AsString())
	require.Equal(t, "amd64", attrs["architecture"].AsString())
	require.Equal(t, "ucrt", attrs["libc"].AsString())
}

func TestEmitWorkerConfigEventHostingEnvironmentVersion(t *testing.T) {
	eventLogger := &captureEventLogger{}

	emitWorkerConfigEvent(eventLogger, namespace.Name("ns"), &workerpb.WorkerHeartbeat{
		WorkerInstanceKey: "w1",
		Environment: &workerpb.EnvironmentInfo{
			HostingEnvironments: []*workerpb.EnvironmentInfo_HostingEnvironment{
				{
					Type:    workerpb.EnvironmentInfo_HostingEnvironment_HOSTING_ENVIRONMENT_TYPE_AZURE_FUNCTIONS,
					Version: "~4",
				},
			},
		},
	})

	attrs := map[string]otellog.Value{}
	eventLogger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value
		return true
	})
	envs := attrs["hosting_environments"].AsSlice()
	require.Len(t, envs, 1)
	env0 := mapValueToMap(envs[0])
	require.Equal(t, "azure_functions", env0["type"].AsString())
	require.Equal(t, "~4", env0["version"].AsString())
}

func TestUnknownEnumValues(t *testing.T) {
	require.Empty(t, libcName(workerpb.EnvironmentInfo_LinuxPlatform_LIBC_UNSPECIFIED))
	require.Empty(t, libcName(workerpb.EnvironmentInfo_LinuxPlatform_Libc(999)))
	require.Empty(t, crtName(workerpb.EnvironmentInfo_WindowsPlatform_CRT_UNSPECIFIED))
	require.Empty(t, crtName(workerpb.EnvironmentInfo_WindowsPlatform_Crt(999)))
}
