package wideevents

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/noop"
)

// instrumentationName is the OTEL instrumentation scope for events emitted by this package.
const instrumentationName = "go.temporal.io/server/common/wideevents"

// Payload is the data of one wide event. Each event type implements it by supplying its stable
// event name and contributing its fields as OTEL log attributes.
type Payload interface {
	// EventName is the stable name of the event type. It becomes the log record's event name so
	// events are easy to trace/grep by type.
	EventName() string
	// Attributes returns the event's fields as OTEL log key-values.
	Attributes() []log.KeyValue
}

// NewLogger returns the OTEL logger used to emit events for serviceName. serviceName is attached
// as an instrumentation-scope attribute (OTEL semconv service.name) so every emitted event carries
// it, replacing per-event common-tag plumbing.
func NewLogger(lp log.LoggerProvider, serviceName string) log.Logger {
	return lp.Logger(
		instrumentationName,
		log.WithInstrumentationAttributes(attribute.String("service.name", serviceName)),
	)
}

// NoopLogger returns a logger that discards all events. Safe default for tests and for
// deployments that have not opted in to a real LoggerProvider.
func NoopLogger() log.Logger {
	return noop.NewLoggerProvider().Logger(instrumentationName)
}

// Emit writes p as a single OTEL log record via logger. A nil logger is a safe no-op so call sites
// never need to guard. Batching, export, and serialization are handled by the LoggerProvider that
// produced logger.
func Emit(logger log.Logger, p Payload) {
	if logger == nil {
		return
	}
	var rec log.Record
	rec.SetSeverity(log.SeverityInfo)
	// The event type is the record's event name so events are easy to trace/grep by type.
	rec.SetEventName(p.EventName())
	rec.AddAttributes(p.Attributes()...)
	logger.Emit(context.Background(), rec)
}
