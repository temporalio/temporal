package wideevents

import (
	"go.opentelemetry.io/otel/log"
)

// Worker events are split into config and metrics because they have different
// lifecycles: config is static per worker, metrics change continuously.
const (
	WorkerConfigEventName  = "worker_config"
	WorkerMetricsEventName = "worker_metrics"
)

// WorkerConfigPayload describes the static properties of a worker.
type WorkerConfigPayload struct {
	Namespace                 string
	TaskQueue                 string
	WorkerInstanceKey         string
	WorkerIdentity            string
	StartTime                 string
	SdkName                   string
	SdkVersion                string
	DeploymentName            string
	BuildID                   string
	Runtimes                  []RuntimeInfo
	HostingEnvironments       []HostingEnvironmentInfo
	OS                        string
	OSVersion                 string
	Architecture              string
	Libc                      string
	Plugins                   []PluginInfo
	Drivers                   []string
	WorkflowPollerAutoscaling bool
	ActivityPollerAutoscaling bool
	NexusPollerAutoscaling    bool
	// Details holds additional properties not covered by the fields above.
	// Empty today; reserved for future extensibility without schema changes.
	Details map[string]any
}

type RuntimeInfo struct {
	Type    string
	Version string
}

type HostingEnvironmentInfo struct {
	Type    string
	Version string
}

type PluginInfo struct {
	Name    string
	Version string
}

func (p WorkerConfigPayload) EventName() string { return WorkerConfigEventName }

func (p WorkerConfigPayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("namespace", p.Namespace),
		log.String("task_queue", p.TaskQueue),
		log.String("worker_instance_key", p.WorkerInstanceKey),
		log.String("worker_identity", p.WorkerIdentity),
		log.String("start_time", p.StartTime),
		log.String("sdk_name", p.SdkName),
		log.String("sdk_version", p.SdkVersion),
		log.String("deployment_name", p.DeploymentName),
		log.String("build_id", p.BuildID),
		mapSliceAttr("runtimes", p.Runtimes, func(r RuntimeInfo) []log.KeyValue {
			return []log.KeyValue{log.String("type", r.Type), log.String("version", r.Version)}
		}),
		mapSliceAttr("hosting_environments", p.HostingEnvironments, func(h HostingEnvironmentInfo) []log.KeyValue {
			return []log.KeyValue{log.String("type", h.Type), log.String("version", h.Version)}
		}),
		log.String("os", p.OS),
		log.String("os_version", p.OSVersion),
		log.String("architecture", p.Architecture),
		log.String("libc", p.Libc),
		mapSliceAttr("plugins", p.Plugins, func(pi PluginInfo) []log.KeyValue {
			return []log.KeyValue{log.String("name", pi.Name), log.String("version", pi.Version)}
		}),
		stringSliceAttr("drivers", p.Drivers),
		log.Bool("workflow_poller_autoscaling", p.WorkflowPollerAutoscaling),
		log.Bool("activity_poller_autoscaling", p.ActivityPollerAutoscaling),
		log.Bool("nexus_poller_autoscaling", p.NexusPollerAutoscaling),
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}

// WorkerTaskTypeStats holds per-task-type capacity data. Pointer fields are
// omitted when the source message is absent, distinguishing "not reported"
// from an actual zero value.
type WorkerTaskTypeStats struct {
	TotalSlots                 *int32 `json:"total_slots,omitempty"`
	UsedSlots                  *int32 `json:"used_slots,omitempty"`
	CurrentPollers             *int32 `json:"current_pollers,omitempty"`
	CurrentStickyPollers       *int32 `json:"current_sticky_pollers,omitempty"`
	TotalProcessedTasks        *int32 `json:"total_processed_tasks,omitempty"`
	TotalFailedTasks           *int32 `json:"total_failed_tasks,omitempty"`
	LastIntervalProcessedTasks *int32 `json:"last_interval_processed_tasks,omitempty"`
	LastIntervalFailureTasks   *int32 `json:"last_interval_failure_tasks,omitempty"`
}

// WorkerMetricsPayload is a point-in-time snapshot of a worker's capacity.
// To compute aggregate capacity for a task queue, take the latest row per
// worker and sum.
type WorkerMetricsPayload struct {
	Namespace         string
	TaskQueue         string
	WorkerInstanceKey string
	TaskTypeStats     map[string]WorkerTaskTypeStats
	// Details holds additional metrics not covered by the fields above.
	// Empty today; reserved for future extensibility without schema changes.
	Details map[string]any
}

func (p WorkerMetricsPayload) EventName() string { return WorkerMetricsEventName }

func (p WorkerMetricsPayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("namespace", p.Namespace),
		log.String("task_queue", p.TaskQueue),
		log.String("worker_instance_key", p.WorkerInstanceKey),
	}
	if len(p.TaskTypeStats) > 0 {
		attrs = append(attrs, jsonAttr("task_type_stats", p.TaskTypeStats))
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}
