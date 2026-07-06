package events

// NamespaceLifecycle is a generic, phase-discriminated wide event describing namespace-level
// activity (replication, failover, configuration, admission, handover, etc.). This package owns
// only the stable envelope: the set of phase values and the contents of Details are supplied by
// the emitter.
var NamespaceLifecycle = NewEventDef("namespace_lifecycle")

// NamespaceLifecyclePayload is the NamespaceLifecycle payload. The identity fields are stable;
// all phase-specific data goes in Details and is emitted as a single nested "details" object.
type NamespaceLifecyclePayload struct {
	Phase       string
	Namespace   string
	NamespaceID string
	Details     map[string]any
}

func (p NamespaceLifecyclePayload) Encode(enc Encoder) {
	enc.String("phase", p.Phase)
	enc.String("namespace", p.Namespace)
	enc.String("namespace_id", p.NamespaceID)
	if len(p.Details) > 0 {
		enc.Any("details", p.Details)
	}
}

// EmitNamespaceLifecycle sends a NamespaceLifecycle event through the handler. A nil handler is a
// safe no-op so call sites never need to guard.
func EmitNamespaceLifecycle(h Handler, p NamespaceLifecyclePayload) {
	if h == nil {
		return
	}
	NamespaceLifecycle.With(h).Emit(p)
}
