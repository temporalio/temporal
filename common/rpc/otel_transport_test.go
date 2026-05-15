package rpc

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestRPCFactoryOTelIntegration(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		f := &RPCFactory{}
		rt := http.DefaultTransport

		// Calling wrapWithOTEL doesn't return a new Transport.
		require.Same(t, rt, f.wrapWithOTEL(rt))
	})

	t.Run("WithOTELTracing", func(t *testing.T) {
		recorder := tracetest.NewSpanRecorder()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		f := &RPCFactory{
			otelTracerProvider: tp,
			otelPropagator:     propagation.TraceContext{},
		}
		rt := http.DefaultTransport
		wrapped := f.wrapWithOTEL(rt)
		require.NotSame(t, rt, wrapped)
	})
}
