package workers

import (
	enumspb "go.temporal.io/api/enums/v1"
	workerpb "go.temporal.io/api/worker/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/tqid"
)

// Values for the poller_kind tag on the per-worker poller metrics.
const (
	pollerKindNormal = "normal"
	pollerKindSticky = "sticky"
)

// WorkerMetricsConfig contains dynamic config flags for worker-related metrics.
type WorkerMetricsConfig struct {
	EnablePluginMetrics            dynamicconfig.BoolPropertyFn
	EnablePollerAutoscalingMetrics dynamicconfig.BoolPropertyFn
	EnablePerWorkerPollerMetrics   dynamicconfig.BoolPropertyFnWithNamespaceFilter
	BreakdownMetricsByTaskQueue    dynamicconfig.BoolPropertyFnWithTaskQueueFilter
	ExternalPayloadsEnabled        dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

// workerMetricsEmitter encapsulates logic for emitting metrics derived from worker heartbeats.
type workerMetricsEmitter struct {
	handler metrics.Handler
	config  WorkerMetricsConfig
}

func (e *workerMetricsEmitter) emit(nsID namespace.ID, nsName namespace.Name, heartbeats []*workerpb.WorkerHeartbeat) {
	// The SDK aggregates all workers on the same Client into one heartbeat RPC, so
	// len(heartbeats) approximates workers-per-process. It's per-Client-per-Namespace,
	// not strictly per-process, but multiple Clients per process is uncommon in practice.
	metrics.WorkerRegistryWorkersPerProcess.With(e.handler).Record(int64(len(heartbeats)))

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

		e.emitPerWorkerPollerMetrics(nsID, nsName, hb)

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

// emitPerWorkerPollerMetrics emits one series per poller for a single worker, identified by
// its worker instance key. That key matches DescribeWorker and ListWorkers, so a series can be
// taken back to the API for the full heartbeat -- though the Prometheus reporter sanitizes label
// values to alphanumerics and underscores, so the key's dashes surface as underscores and have
// to be converted back first.
//
// The cost of that exactness: the instance key is a UUID regenerated every time a worker is
// constructed, so every worker restart and every deployment mints a fresh set of series, and a
// worker bounce shows up as a new line rather than a continuation. Emitted series are never
// reclaimed either, because neither metrics backend evicts a tag combination once seen: turning
// the setting off stops new series but leaves existing ones exported at their last value until
// the matching hosts restart. All of a namespace's heartbeats are handled by one matching host,
// so that cost lands on a single process. Enable it for diagnosis, not steady state.
func (e *workerMetricsEmitter) emitPerWorkerPollerMetrics(nsID namespace.ID, nsName namespace.Name, hb *workerpb.WorkerHeartbeat) {
	// System workers are excluded to match ListWorkers, which hides them by default.
	if e.config.EnablePerWorkerPollerMetrics == nil ||
		!e.config.EnablePerWorkerPollerMetrics(nsName.String()) ||
		primitives.IsInternalTaskQueue(hb.GetTaskQueue()) {
		return
	}
	family, err := tqid.NewTaskQueueFamily(nsID.String(), hb.GetTaskQueue())
	if err != nil {
		return
	}

	// Sticky workflow pollers have no task queue type of their own, so the poller kind is what
	// separates them from normal workflow pollers.
	//
	// A zero target means the poller has no autoscaler, either because its count is fixed or
	// because the worker does not run it at all -- the SDK fills all four poller blocks whether
	// or not the matching sub-worker exists. Autoscalers clamp their target to at least one, so
	// skipping zero drops exactly those cases and nothing else.
	recordPoller := func(taskType enumspb.TaskQueueType, info *workerpb.WorkerPollerInfo, kind string) {
		if info.GetTargetPollers() <= 0 {
			return
		}
		tq := family.TaskQueue(taskType)
		breakdownByTQ := e.config.BreakdownMetricsByTaskQueue != nil &&
			e.config.BreakdownMetricsByTaskQueue(nsName.String(), hb.GetTaskQueue(), taskType)
		handler := metrics.GetPerTaskQueueScope(e.handler, nsName.String(), tq, breakdownByTQ,
			metrics.PollerKindTag(kind),
			metrics.WorkerInstanceKeyTag(hb.GetWorkerInstanceKey()))
		metrics.WorkerPollerTarget.With(handler).Record(float64(info.GetTargetPollers()))
	}

	recordPoller(enumspb.TASK_QUEUE_TYPE_WORKFLOW, hb.GetWorkflowPollerInfo(), pollerKindNormal)
	recordPoller(enumspb.TASK_QUEUE_TYPE_WORKFLOW, hb.GetWorkflowStickyPollerInfo(), pollerKindSticky)
	recordPoller(enumspb.TASK_QUEUE_TYPE_ACTIVITY, hb.GetActivityPollerInfo(), pollerKindNormal)
	recordPoller(enumspb.TASK_QUEUE_TYPE_NEXUS, hb.GetNexusPollerInfo(), pollerKindNormal)
}
