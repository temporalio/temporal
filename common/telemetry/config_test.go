// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package telemetry_test

import (
	"context"
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
	err := yaml.Unmarshal([]byte(basicOTLPTraceOnlyConfig), &cfg)
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
