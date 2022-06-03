package telemetry

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type (
	ServerTraceInterceptor grpc.UnaryServerInterceptor
	ClientTraceInterceptor grpc.UnaryClientInterceptor
)

func NewServerTraceInterceptor(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
) ServerTraceInterceptor {
	return ServerTraceInterceptor(
		otelgrpc.UnaryServerInterceptor(
			otelgrpc.WithPropagators(tmp),
			otelgrpc.WithTracerProvider(tp),
		),
	)
}

func NewClientTraceInterceptor(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
) ClientTraceInterceptor {
	return ClientTraceInterceptor(
		otelgrpc.UnaryClientInterceptor(
			otelgrpc.WithPropagators(tmp),
			otelgrpc.WithTracerProvider(tp),
		),
	)
}
