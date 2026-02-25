package workers

import (
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

// WorkerMetricsConfig contains dynamic config flags for worker-related metrics.
type WorkerMetricsConfig struct {
	EnablePluginMetrics            dynamicconfig.BoolPropertyFn
	EnablePollerAutoscalingMetrics dynamicconfig.BoolPropertyFn
}

// workerMetricsEmitter encapsulates logic for emitting metrics derived from worker heartbeats.
type workerMetricsEmitter struct {
	handler metrics.Handler
	config  WorkerMetricsConfig
}

func (e *workerMetricsEmitter) emit(nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	enablePluginMetrics := e.config.EnablePluginMetrics != nil && e.config.EnablePluginMetrics()
	enablePollerAutoscalingMetrics := e.config.EnablePollerAutoscalingMetrics != nil && e.config.EnablePollerAutoscalingMetrics()

	recordedPlugins := make(map[string]bool)
	recordedAutoscaling := make(map[string]bool)

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
						Record(1, metrics.NamespaceIDTag(nsName.String()), metrics.WorkerPluginNameTag(pluginName))
					recordedPlugins[pluginName] = true
				}
			}
		}

		// Poller autoscaling metrics (if enabled)
		if enablePollerAutoscalingMetrics {
			e.emitPollerAutoscaling(nsName, hb.WorkflowPollerInfo, metrics.PollerTypeWorkflow, recordedAutoscaling)
			e.emitPollerAutoscaling(nsName, hb.ActivityPollerInfo, metrics.PollerTypeActivity, recordedAutoscaling)
			e.emitPollerAutoscaling(nsName, hb.NexusPollerInfo, metrics.PollerTypeNexus, recordedAutoscaling)
		}
	}
}

func (e *workerMetricsEmitter) emitPollerAutoscaling(
	nsName namespace.Name,
	pollerInfo *workerpb.WorkerPollerInfo,
	pollerType string,
	recorded map[string]bool,
) {
	if pollerInfo != nil && pollerInfo.IsAutoscaling && !recorded[pollerType] {
		metrics.PollerAutoscalingEnabledMetric.
			With(e.handler).
			Record(1, metrics.NamespaceIDTag(nsName.String()), metrics.PollerTypeTag(pollerType))
		recorded[pollerType] = true
	}
}
