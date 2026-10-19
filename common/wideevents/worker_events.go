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
