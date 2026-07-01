package events

type noopHandler struct{}
type noopEmitter struct{}

// NoopHandler returns a Handler that discards all events. Safe default for tests.
func NoopHandler() Handler { return noopHandler{} }

func (noopHandler) Event(string) Emitter    { return noopEmitter{} }
func (noopHandler) WithTags(...Tag) Handler { return noopHandler{} }
func (noopEmitter) Emit(Payload)            {}
