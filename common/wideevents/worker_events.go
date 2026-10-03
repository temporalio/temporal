package wideevents

import (
	"go.opentelemetry.io/otel/log"
)

const WorkerEnvironmentEventName = "worker_environment"

// WorkerEnvironmentPayload describes the runtime environment of a worker, emitted when
// a heartbeat includes environment info.
type WorkerEnvironmentPayload struct {
	Namespace    string
	RuntimeType  string
	OS           string
	Architecture string
}

func (p WorkerEnvironmentPayload) EventName() string { return WorkerEnvironmentEventName }

func (p WorkerEnvironmentPayload) Attributes() []log.KeyValue {
	return []log.KeyValue{
		log.String("namespace", p.Namespace),
		log.String("runtime_type", p.RuntimeType),
		log.String("os", p.OS),
		log.String("architecture", p.Architecture),
	}
}
