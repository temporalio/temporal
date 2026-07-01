package events

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

// logHandler is the default Handler: it emits each event as one structured log line whose
// message is the event type (for easy tracing/grepping) and whose "payload" tag is a map of
// the event's fields.
type logHandler struct {
	logger log.Logger
	tags   []Tag
}

// NewLogHandler returns a Handler that logs each event as JSON-able fields via logger.Info.
func NewLogHandler(logger log.Logger) Handler {
	return &logHandler{logger: logger}
}

func (h *logHandler) Event(name string) Emitter {
	return &logEmitter{logger: h.logger, name: name, tags: h.tags}
}

func (h *logHandler) WithTags(tags ...Tag) Handler {
	merged := make([]Tag, 0, len(h.tags)+len(tags))
	merged = append(merged, h.tags...)
	merged = append(merged, tags...)
	return &logHandler{logger: h.logger, tags: merged}
}

type logEmitter struct {
	logger log.Logger
	name   string
	tags   []Tag
}

func (e *logEmitter) Emit(p Payload) {
	enc := newJSONEncoder()
	for _, t := range e.tags {
		enc.String(t.Key, t.Value)
	}
	p.Encode(enc)
	// The event type is the log message so events are easy to trace/grep by type.
	e.logger.Info(e.name, tag.NewAnyTag("payload", enc.fields))
}
