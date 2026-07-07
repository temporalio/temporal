package events

// eventDefinition is the registered identity (name) of an event type.
type eventDefinition struct {
	name string
}

// NewEventDef defines an event type and registers it into the global catalog.
// Use as a package-level var: var MyEvent = NewEventDef("my_event").
func NewEventDef(name string) eventDefinition {
	def := eventDefinition{name: name}
	globalRegistry.register(def)
	return def
}

// With binds this event definition to a handler, returning an Emitter for its name.
// Call site idiom: events.MyEvent.With(handler).Emit(MyPayload{...}).
func (d eventDefinition) With(h Handler) Emitter {
	return h.Event(d.name)
}

// Name returns the event's registered name.
func (d eventDefinition) Name() string { return d.name }
