package telemetry_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/telemetry"
	"go.uber.org/fx"
)

const svcname = "io.temporal.some_service"

type CapturingExporter struct {
	*tracetest.InMemoryExporter
}

// Shutdown hides the default Shutdown behavior of the InMemoryExporter
// which is to clear the in-memory set of captured spans. We don't want that.
func (ce CapturingExporter) Shutdown(context.Context) error {
	return nil
}

func TestServiceModule(t *testing.T) {
	sink := CapturingExporter{tracetest.NewInMemoryExporter()}
	fx.New(
		telemetry.GlobalModule,
		telemetry.ServiceModule(svcname),

		// replace batching otlp span exporter with a synchronous in-memory
		// version of the same interfaces
		fx.Replace(fx.Annotate(
			sdktrace.NewSimpleSpanProcessor(sink),
			fx.As(new(sdktrace.SpanProcessor)))),

		// create a single span and then shutdown
		fx.Invoke(func(tp trace.TracerProvider, shutdowner fx.Shutdowner) {
			_, span := tp.Tracer(t.Name()).Start(context.TODO(), "span0")
			span.End()
			shutdowner.Shutdown()
		}),
	).Run()

	spans := sink.GetSpans()
	require.Len(t, spans, 1)
	require.Equal(t, "span0", spans[0].Name)
	require.Contains(t,
		spans[0].Resource.Attributes(),
		semconv.ServiceNameKey.String(svcname))
	require.Equal(t, spans[0].InstrumentationLibrary.Name, t.Name())
}
