package telemetry

import (
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

// GlobalModule holds process-global telemetry fx state. The following
// types can be overriden with fx.Replace/fx.Decorate:
// - go.opentelemetry.io/otel/sdk/trace.SpanExporter
var GlobalModule = fx.Module("io.temporal.telemetry.Global", fx.Provide(newExporter))

// ServiceModule holds per-service (i.e. frontend/history/matching/worker) fx
// state. The following types can be overriden with fx.Replace/fx.Decorate:
// - []go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc.Option
// - go.opentelemetry.io/otel/sdk/trace.SpanProcessor
// - go.opentelemetry.io/otel/trace.TracerProvider
// - *go.opentelemetry.io/otel/sdk/resource.Resource
// - []go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/ogelgrpc.Option
//
// If the TracerProvider provided by this module supports graceful shutdown,
// that function is invoked when the module is stopped.
func ServiceModule(serviceName string) fx.Option {
	grpcInstrumentationOpts := func(tp trace.TracerProvider) []otelgrpc.Option {
		return []otelgrpc.Option{otelgrpc.WithTracerProvider(tp)}
	}

	shutdownTracing := func(lc fx.Lifecycle, tp trace.TracerProvider) {
		type shutdowner interface{ Shutdown(context.Context) error }
		if shutdowner, ok := tp.(shutdowner); ok {
			lc.Append(fx.Hook{OnStop: shutdowner.Shutdown})
		}
	}

	return fx.Module(
		serviceName,
		fx.Supply(mustParseServiceName(serviceName)),
		fx.Provide(grpcExporterOpts),
		fx.Provide(newSpanProcessor),
		fx.Provide(newResource),
		fx.Provide(newTracerProvider),
		fx.Provide(grpcInstrumentationOpts),
		fx.Invoke(shutdownTracing),
	)
}
