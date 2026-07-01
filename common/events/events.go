package events

// Encoder writes an event's fields. A Handler supplies the concrete encoder.
type Encoder interface {
	String(key, value string)
	Int64(key string, value int64)
	Float64(key string, value float64)
	Bool(key string, value bool)
	// Any records an arbitrary structured value under key, e.g. a whole object.
	Any(key string, value any)
}

// Payload is the data of one wide event. Each event type implements it.
type Payload interface {
	Encode(enc Encoder)
}

// Tag is a common field applied to every event emitted through a handler.
type Tag struct {
	Key   string
	Value string
}

// Handler is the pluggable emitter for structured events.
type Handler interface {
	Event(name string) Emitter
	WithTags(tags ...Tag) Handler
}

// Emitter is bound to a single event name and emits payloads for it.
type Emitter interface {
	Emit(p Payload)
}
