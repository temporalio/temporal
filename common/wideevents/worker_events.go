package wideevents

import (
	"strings"

	"go.opentelemetry.io/otel/log"
)

const WorkerConfigEventName = "worker_config"

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
