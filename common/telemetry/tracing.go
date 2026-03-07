package telemetry

import (
	"go.opentelemetry.io/otel/trace"
	otelnoop "go.opentelemetry.io/otel/trace/noop"
)

// IsTracingEnabled reports whether the given TracerProvider will produce real traces.
func IsTracingEnabled(tp trace.TracerProvider) bool {
	_, isNoop := tp.(otelnoop.TracerProvider)
	return !isNoop
}
