package telemetry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.opentelemetry.io/otel/trace"
)

func init() {
	// make this global - it doesn't vary by service
	// intentionally omitting baggage propagation until we need it.
	otel.SetTextMapPropagator(propagation.TraceContext{})
}

type serviceName string

func (sn serviceName) String() string {
	return string(sn)
}

func mustParseServiceName(str string) serviceName {
	result, err := parseServiceName(str)
	if err != nil {
		panic(err)
	}
	return result
}

func parseServiceName(str string) (serviceName, error) {
	if str == "" {
		return "", errors.New("empty service name")
	}
	if !strings.HasPrefix(str, "io.temporal.") {
		str = fmt.Sprintf("io.temporal.%v", str)
	}
	return serviceName(str), nil
}

func grpcExporterOpts() []otlptracegrpc.Option {
	return []otlptracegrpc.Option{
		// this is the same as the default - just here for clarity
		otlptracegrpc.WithEndpoint("localhost:4317"),
		otlptracegrpc.WithInsecure(),
	}
}

func newExporter(opts []otlptracegrpc.Option) (sdktrace.SpanExporter, error) {
	return otlptrace.New(context.Background(), otlptracegrpc.NewClient(opts...))
}

func newTracerProvider(
	resource *resource.Resource,
	sp sdktrace.SpanProcessor,
) (trace.TracerProvider, error) {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sp),
		sdktrace.WithResource(resource),
	), nil
}

func newSpanProcessor(exp sdktrace.SpanExporter) sdktrace.SpanProcessor {
	return sdktrace.NewBatchSpanProcessor(exp)
}

func newResource(serviceName serviceName) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(serviceName.String())))
}
