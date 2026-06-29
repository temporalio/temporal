package events

// FieldType is the declared type of an event field, recorded as schema metadata.
type FieldType int

const (
	FieldString FieldType = iota
	FieldInt64
	FieldFloat64
	FieldBool
	FieldAny
)

type fieldSpec struct {
	name string
	typ  FieldType
}

// eventDefinition is the registered identity (name + schema) of an event type.
type eventDefinition struct {
	name   string
	fields []fieldSpec
}

// Option customizes an eventDefinition.
type Option func(*eventDefinition)

// WithField records a field's name and type in the event's schema metadata.
func WithField(name string, t FieldType) Option {
	return func(d *eventDefinition) {
		d.fields = append(d.fields, fieldSpec{name: name, typ: t})
	}
}

func newEventDefinition(name string, opts ...Option) eventDefinition {
	d := eventDefinition{name: name}
	for _, opt := range opts {
		opt(&d)
	}
	return d
}

// NewEventDef defines an event type and registers it into the global catalog.
// Use as a package-level var: var MyEvent = NewEventDef("my_event", WithField(...)).
func NewEventDef(name string, opts ...Option) eventDefinition {
	def := newEventDefinition(name, opts...)
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
