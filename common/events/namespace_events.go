package events

import "go.opentelemetry.io/otel/log"

// NamespaceLifecycleEventName is the stable event name for the generic, phase-discriminated wide
// event describing namespace-level activity (replication, failover, configuration, admission,
// handover, etc.). This package owns only the stable envelope: the set of phase values and the
// contents of Details are supplied by the emitter.
const NamespaceLifecycleEventName = "namespace_lifecycle"

// NamespaceLifecyclePayload is the NamespaceLifecycle payload. The identity fields are stable;
// all phase-specific data goes in Details and is emitted as a single nested "details" object.
type NamespaceLifecyclePayload struct {
	Phase       string
	Namespace   string
	NamespaceID string
	Details     map[string]any
}

func (p NamespaceLifecyclePayload) EventName() string { return NamespaceLifecycleEventName }

func (p NamespaceLifecyclePayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("phase", p.Phase),
		log.String("namespace", p.Namespace),
		log.String("namespace_id", p.NamespaceID),
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}
