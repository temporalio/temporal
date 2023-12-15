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
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/telemetry"
)

func TestEnvSpanExporter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		envVars  map[string]string
		errMsg   string
		exporter bool
	}{
		{
			name:    "NoExporter",
			envVars: map[string]string{},
		},
		{
			name: "InvalidExporter",
			envVars: map[string]string{
				telemetry.OtelTracesExporterEnvKey: "invalid-exporter",
			},
			errMsg: "unsupported OpenTelemetry env var: OTEL_TRACES_EXPORTER=invalid-exporter",
		},
		{
			name: "ValidExporter",
			envVars: map[string]string{
				telemetry.OtelTracesExporterEnvKey: "oltp",
			},
			exporter: true,
		},
		{
			name: "ValidExporterWithValidProtocol",
			envVars: map[string]string{
				telemetry.OtelTracesExporterEnvKey:         "oltp",
				telemetry.OtelTracesExporterProtocolEnvKey: "grpc",
			},
			errMsg:   "",
			exporter: true,
		},
		{
			name: "ValidExporterWithInvalidProtocol",
			envVars: map[string]string{
				telemetry.OtelTracesExporterEnvKey:         "oltp",
				telemetry.OtelTracesExporterProtocolEnvKey: "invalid-protocol",
			},
			errMsg: "unsupported OpenTelemetry env var: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=invalid-protocol",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			envVarsLookup := func(key string) (string, bool) {
				val, ok := test.envVars[key]
				return val, ok
			}

			exp, err := telemetry.EnvSpanExporter(envVarsLookup)
			if test.errMsg == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, test.errMsg)
			}
			if test.exporter {
				require.NotNil(t, exp)
			}
		})
	}
}

func TestResourceServiceName(t *testing.T) {
	t.Run("DefaultPrefix", func(t *testing.T) {
		require.Equal(t,
			"io.temporal.history",
			telemetry.ResourceServiceName(primitives.HistoryService, func(key string) (string, bool) {
				return "", false
			}),
		)
	})

	t.Run("SingleFrontendServiceName", func(t *testing.T) {
		require.Equal(t,
			"io.temporal.frontend", // instead of "internal-frontend"
			telemetry.ResourceServiceName(primitives.InternalFrontendService, func(key string) (string, bool) {
				return "", false
			}),
		)
	})

	t.Run("CustomPrefix", func(t *testing.T) {
		require.Equal(t,
			"PREFIX.matching",
			telemetry.ResourceServiceName(primitives.MatchingService, func(key string) (string, bool) {
				require.Equal(t, telemetry.OtelServiceNameEnvKey, key)
				return "PREFIX", true
			}),
		)
	})
}
