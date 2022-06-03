package telemetry

import (
	"context"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type starter interface{ Start(context.Context) error }

func Starter(exporters []sdktrace.SpanExporter) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for _, e := range exporters {
			if starter, ok := e.(starter); ok {
				err := starter.Start(ctx)
				if err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func Stopper(exporters []sdktrace.SpanExporter) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		for _, e := range exporters {
			err := e.Shutdown(ctx)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

func valueOrDefault[T comparable](v, defval T) T {
	var zero T
	if v == zero {
		return defval
	}
	return v
}
