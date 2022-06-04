package telemetry_test

import (
	"context"
	"strings"
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
      proto: grpc
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

func TestEmptyConfig(t *testing.T) {
	cfg := telemetry.ExportConfig{}
	exporters, err := cfg.SpanExporters(context.TODO())
	require.NoError(t, err)
	require.Len(t, exporters, 0)
}

func TestOTLPTraceGRPC(t *testing.T) {
	cfg := telemetry.PrivateExportConfig{}
	d := yaml.NewDecoder(strings.NewReader(basicOTLPTraceOnlyConfig))
	err := d.Decode(&cfg)
	require.NoError(t, err)
	require.Len(t, cfg.Connections, 0)
	require.Len(t, cfg.Exporters, 1)

	exp := cfg.Exporters[0]
	require.Equal(t, exp.Kind.Signal, "traces")
	require.Equal(t, exp.Kind.Model, "otlp")
	require.Equal(t, exp.Kind.Proto, "grpc")
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
