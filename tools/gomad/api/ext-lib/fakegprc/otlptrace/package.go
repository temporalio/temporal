package otlpmetric

import (
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	fakegrpc "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
)

func WithDialOption(opts ...fakegrpc.DialOption) otlptracegrpc.Option {
	// TODO
	return nil
}

func WithGRPCConn(conn *fakegrpc.ClientConn) otlptracegrpc.Option {
	// TODO
	return nil
}
