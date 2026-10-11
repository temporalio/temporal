package wideevents

import (
	"strings"

	"go.opentelemetry.io/otel/log"
)

// Worker events are split into config and metrics because they have different
// lifecycles: config is static per worker, metrics change continuously.
// Combining them would leave config fields empty on most rows, complicating
// queries that aggregate by runtime/os/architecture.
const (
	WorkerConfigEventName  = "worker_config"
	WorkerMetricsEventName = "worker_metrics"
)

// WorkerConfigPayload describes the static properties of a worker: runtime,
// OS, and architecture.
type WorkerConfigPayload struct {
	Namespace                 string
	TaskQueue                 string
	WorkerInstanceKey         string
	SdkName                   string
	SdkVersion                string
	DeploymentName            string
	BuildID                   string
	Runtimes                  []string
	HostingEnvironments       []string
	OS                        string
	Architecture              string
	WorkflowPollerAutoscaling bool
	ActivityPollerAutoscaling bool
	NexusPollerAutoscaling    bool
}

func (p WorkerConfigPayload) EventName() string { return WorkerConfigEventName }

func (p WorkerConfigPayload) Attributes() []log.KeyValue {
	return []log.KeyValue{
		log.String("namespace", p.Namespace),
		log.String("task_queue", p.TaskQueue),
		log.String("worker_instance_key", p.WorkerInstanceKey),
		log.String("sdk_name", p.SdkName),
		log.String("sdk_version", p.SdkVersion),
		log.String("deployment_name", p.DeploymentName),
		log.String("build_id", p.BuildID),
		log.String("runtimes", strings.Join(p.Runtimes, ",")),
		log.String("hosting_environments", strings.Join(p.HostingEnvironments, ",")),
		log.String("os", p.OS),
		log.String("architecture", p.Architecture),
		log.Bool("workflow_poller_autoscaling", p.WorkflowPollerAutoscaling),
		log.Bool("activity_poller_autoscaling", p.ActivityPollerAutoscaling),
		log.Bool("nexus_poller_autoscaling", p.NexusPollerAutoscaling),
	}
}

// WorkerTaskTypeStats holds per-task-type capacity data.
type WorkerTaskTypeStats struct {
	TotalSlots  int32 `json:"total_slots"`
	UsedSlots   int32 `json:"used_slots"`
	PollerCount int32 `json:"poller_count"`
}

// WorkerMetricsPayload is a point-in-time snapshot of a worker's capacity.
// To compute aggregate capacity for a task queue, take the latest row per
// worker and sum.
type WorkerMetricsPayload struct {
	Namespace         string
	TaskQueue         string
	WorkerInstanceKey string
	TaskTypeStats     map[string]WorkerTaskTypeStats
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
	return attrs
}
