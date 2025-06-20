package telemetry_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/telemetry"
	"gopkg.in/yaml.v3"
)

var basicOTLPTraceOnlyConfig = `
exporters:
  - kind:
      signal: traces
      model: otlp
      protocol: grpc
    spec:
      headers:
        a: b
        c: d
      timeout: 10s
      retry:
        enabled: true
        initial_interval: 1s
        max_interval: 1s
        max_elapsed_time: 1s
      connection:
        block: false
        insecure: true
        endpoint: localhost:4317
`

var sharedConnOTLPConfig = `
otel:
  connections:
    - kind: grpc
      metadata:
        name: conn1
      spec:
        endpoint: localhost:4317
  exporters:
    - kind:
        signal: traces
        model: otlp
        protocol: grpc
      spec:
        connection_name: conn1
    - kind:
        signal: metrics
        model: otlp
        protocol: grpc
      spec:
        connection_name: conn1
`

func TestEmptyConfig(t *testing.T) {
	cfg := telemetry.ExportConfig{}
	exporters, err := cfg.SpanExporters()
	require.NoError(t, err)
	require.Len(t, exporters, 0)
}

func TestExportersWithSharedConn(t *testing.T) {
	root := struct{ Otel telemetry.PrivateExportConfig }{}
	err := yaml.Unmarshal([]byte(sharedConnOTLPConfig), &root)
	require.NoError(t, err)
	cfg := &root.Otel

	spanExporters, err := cfg.SpanExporters()
	require.NoError(t, err)
	require.Len(t, spanExporters, 1)

	metricExporters, err := cfg.MetricExporters()
	require.NoError(t, err)
	require.Len(t, metricExporters, 1)
}

func TestSharedConn(t *testing.T) {
	root := struct{ Otel telemetry.PrivateExportConfig }{}
	err := yaml.Unmarshal([]byte(sharedConnOTLPConfig), &root)
	require.NoError(t, err)
	cfg := &root.Otel
	require.Len(t, cfg.Connections, 1)
	require.Len(t, cfg.Exporters, 2)

	exp := cfg.Exporters[0]
	require.Equal(t, exp.Kind.Signal, "traces")
	require.Equal(t, exp.Kind.Model, "otlp")
	require.Equal(t, exp.Kind.Protocol, "grpc")
	require.NotNil(t, exp.Spec)
	sspec, ok := exp.Spec.(*telemetry.OTLPGRPCSpanExporter)
	require.True(t, ok)
	require.Equal(t, "conn1", sspec.ConnectionName)

	exp = cfg.Exporters[1]
	require.Equal(t, exp.Kind.Signal, "metrics")
	require.Equal(t, exp.Kind.Model, "otlp")
	require.Equal(t, exp.Kind.Protocol, "grpc")
	require.NotNil(t, exp.Spec)
	mspec, ok := exp.Spec.(*telemetry.OTLPGRPCMetricExporter)
	require.True(t, ok)
	require.Equal(t, "conn1", mspec.ConnectionName)
}

func TestOTLPTraceGRPC(t *testing.T) {
	cfg := telemetry.PrivateExportConfig{}
	err := yaml.Unmarshal([]byte(basicOTLPTraceOnlyConfig), &cfg)
	require.NoError(t, err)
	require.Len(t, cfg.Connections, 0)
	require.Len(t, cfg.Exporters, 1)

	exp := cfg.Exporters[0]
	require.Equal(t, exp.Kind.Signal, "traces")
	require.Equal(t, exp.Kind.Model, "otlp")
	require.Equal(t, exp.Kind.Protocol, "grpc")
	require.NotNil(t, exp.Spec)

	spec, ok := exp.Spec.(*telemetry.OTLPGRPCSpanExporter)
	require.True(t, ok)
	require.Equal(t, map[string]string{"a": "b", "c": "d"}, spec.Headers)
	require.Equal(t, 10*time.Second, spec.Timeout)
	require.True(t, spec.Retry.Enabled)
	require.Equal(t, time.Second, spec.Retry.InitialInterval)
	require.Equal(t, time.Second, spec.Retry.MaxInterval)
	require.Equal(t, time.Second, spec.Retry.MaxElapsedTime)

	conn := spec.Connection
	require.True(t, conn.Insecure)
	require.Equal(t, "localhost:4317", conn.Endpoint)
	require.False(t, conn.Block)
}
