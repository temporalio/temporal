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
	Runtimes                  map[string]string // type → version
	HostingEnvironments       map[string]string // type → version
	OS                        string
	OSVersion                 string
	Architecture              string
	Libc                      string
	Plugins                   map[string]string // name → version
	Drivers                   []string
	WorkflowPollerAutoscaling bool
	ActivityPollerAutoscaling bool
	NexusPollerAutoscaling    bool
	// Details holds additional properties not covered by the fields above.
	// Empty today; reserved for future extensibility without schema changes.
	Details map[string]any
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
		stringMapAttr("runtimes", p.Runtimes),
		stringMapAttr("hosting_environments", p.HostingEnvironments),
		log.String("os", p.OS),
		log.String("os_version", p.OSVersion),
		log.String("architecture", p.Architecture),
		log.String("libc", p.Libc),
		stringMapAttr("plugins", p.Plugins),
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
	TotalSlots           *int32
	UsedSlots            *int32
	CurrentPollers       *int32
	CurrentStickyPollers *int32
	TotalProcessedTasks  *int32
	TotalFailedTasks     *int32
}

// appendAttributes adds one attribute per reported stat, prefixed with the
// task type (e.g. workflow_used_slots).
func (s WorkerTaskTypeStats) appendAttributes(attrs []log.KeyValue, taskType string) []log.KeyValue {
	appendStat := func(name string, v *int32) {
		if v != nil {
			attrs = append(attrs, log.Int64(taskType+"_"+name, int64(*v)))
		}
	}
	appendStat("total_slots", s.TotalSlots)
	appendStat("used_slots", s.UsedSlots)
	appendStat("current_pollers", s.CurrentPollers)
	appendStat("current_sticky_pollers", s.CurrentStickyPollers)
	appendStat("total_processed_tasks", s.TotalProcessedTasks)
	appendStat("total_failed_tasks", s.TotalFailedTasks)
	return attrs
}

// WorkerMetricsPayload is a point-in-time snapshot of a worker's metrics.
// Stats are flat attributes, one per task type and stat, so each one can be
// stored and queried as its own typed column.
// Tip: to aggregate for a task queue, take the latest row per worker and
// sum. Handle unknown total_slots (-1) separately.
type WorkerMetricsPayload struct {
	Namespace         string
	TaskQueue         string
	WorkerInstanceKey string
	Workflow          WorkerTaskTypeStats
	Activity          WorkerTaskTypeStats
	Nexus             WorkerTaskTypeStats
	LocalActivity     WorkerTaskTypeStats
	StickyCacheSize   int32
	StickyCacheHit    int32
	StickyCacheMiss   int32
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
	attrs = p.Workflow.appendAttributes(attrs, "workflow")
	attrs = p.Activity.appendAttributes(attrs, "activity")
	attrs = p.Nexus.appendAttributes(attrs, "nexus")
	attrs = p.LocalActivity.appendAttributes(attrs, "local_activity")
	attrs = append(attrs,
		log.Int64("sticky_cache_size", int64(p.StickyCacheSize)),
		log.Int64("sticky_cache_hit", int64(p.StickyCacheHit)),
		log.Int64("sticky_cache_miss", int64(p.StickyCacheMiss)),
	)
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}
