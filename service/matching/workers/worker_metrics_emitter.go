package workers

import (
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tqid"
)

// WorkerMetricsConfig contains dynamic config flags for worker-related metrics.
type WorkerMetricsConfig struct {
	EnablePluginMetrics            dynamicconfig.BoolPropertyFn
	EnablePollerAutoscalingMetrics dynamicconfig.BoolPropertyFn
	BreakdownMetricsByTaskQueue    dynamicconfig.BoolPropertyFnWithTaskQueueFilter
	ExternalPayloadsEnabled        dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// workerMetricsEmitter encapsulates logic for emitting metrics derived from worker heartbeats.
type workerMetricsEmitter struct {
	handler metrics.Handler
	config  WorkerMetricsConfig
}

func (e *workerMetricsEmitter) emit(nsID namespace.ID, nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	enablePluginMetrics := e.config.EnablePluginMetrics != nil && e.config.EnablePluginMetrics()
	enablePollerAutoscalingMetrics := e.config.EnablePollerAutoscalingMetrics != nil && e.config.EnablePollerAutoscalingMetrics()
	enableStorageDriverMetrics := e.config.ExternalPayloadsEnabled != nil && e.config.ExternalPayloadsEnabled(nsName.String())

	recordedPlugins := make(map[string]bool)
	recordedDrivers := make(map[string]bool)

	for _, hb := range heartbeats {
		// Activity slots metric (always enabled)
		if hb.ActivityTaskSlotsInfo != nil {
			metrics.WorkerRegistryActivitySlotsUsed.With(e.handler).Record(int64(hb.ActivityTaskSlotsInfo.CurrentUsedSlots))
		}

		// Plugin metrics (if enabled)
		if enablePluginMetrics {
			for _, pluginInfo := range hb.Plugins {
				pluginName := pluginInfo.Name
				if !recordedPlugins[pluginName] {
					metrics.WorkerPluginNameMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerPluginNameTag(pluginName))
					recordedPlugins[pluginName] = true
				}
			}
		}

		// Poller autoscaling metrics (if enabled)
		if enablePollerAutoscalingMetrics {
			e.emitPollerAutoscaling(nsID, nsName, hb)
		}

		// Storage driver metrics (if external payloads enabled)
		if enableStorageDriverMetrics {
			for _, driver := range hb.GetDrivers() {
				driverType := driver.GetType()
				if !recordedDrivers[driverType] {
					metrics.WorkerStorageDriverTypeMetric.
						With(e.handler).
						Record(1, metrics.NamespaceTag(nsName.String()), metrics.WorkerStorageDriverTypeTag(driverType))
					recordedDrivers[driverType] = true
				}
			}
		}
	}
}

func (e *workerMetricsEmitter) emitPollerAutoscaling(nsID namespace.ID, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	recordAutoscaling := func(taskType enumspb.TaskQueueType) {
		tq := family.TaskQueue(taskType)
		breakdownByTQ := e.config.BreakdownMetricsByTaskQueue != nil &&
			e.config.BreakdownMetricsByTaskQueue(nsName.String(), hb.GetTaskQueue(), taskType)
		handler := metrics.GetPerTaskQueueScope(e.handler, nsName.String(), tq, breakdownByTQ)
		metrics.PollerAutoscalingHeartbeatCount.With(handler).Record(1)
	}

	if hb.WorkflowPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
	if hb.ActivityPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	}
	if hb.NexusPollerInfo.GetIsAutoscaling() {
		recordAutoscaling(enumspb.TASK_QUEUE_TYPE_NEXUS)
	}
}
