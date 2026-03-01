package otlpmetric

import (
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	fakegrpc "go.temporal.io/server/tools/gomad/api/ext-lib/fakegprc"
)

func WithDialOption(opts ...fakegrpc.DialOption) otlpmetricgrpc.Option {
	// TODO
	return nil
}

func WithGRPCConn(conn *fakegrpc.ClientConn) otlpmetricgrpc.Option {
	// TODO
	return nil
}
