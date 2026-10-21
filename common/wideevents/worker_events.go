package wideevents

import (
	"go.opentelemetry.io/otel/log"
)

const WorkerConfigEventName = "worker_config"

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
