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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/telemetry"
)

func TestSupplementTraceExportersFromEnv(t *testing.T) {
	t.Run("when env variable specifies OTEL exporter type, add exporter", func(t *testing.T) {
		exporters := map[telemetry.SpanExporterType]otelsdktrace.SpanExporter{}

		err := telemetry.SupplementTraceExportersFromEnv(
			exporters,
			func(key string) (string, bool) {
				require.Equal(t, telemetry.OtelTracesExporterEnvKey, key)
				return string(telemetry.OtelTracesOtlpExporterType), true
			})

		require.NoError(t, err)
		require.Len(t, exporters, 1)
	})

	t.Run("when env variable specifies OTEL exporter type but the type already exists, don't add exporter", func(t *testing.T) {
		var mockExporter otelsdktrace.SpanExporter
		exporters := map[telemetry.SpanExporterType]otelsdktrace.SpanExporter{
			telemetry.OtelTracesOtlpExporterType: mockExporter,
		}

		err := telemetry.SupplementTraceExportersFromEnv(
			exporters,
			func(key string) (string, bool) {
				return string(telemetry.OtelTracesOtlpExporterType), true
			})

		require.NoError(t, err)
		require.Equal(t, exporters[telemetry.OtelTracesOtlpExporterType], mockExporter)
	})

	t.Run("when env variable is specified but exporter type is not supported, return error", func(t *testing.T) {
		exporters := map[telemetry.SpanExporterType]otelsdktrace.SpanExporter{}

		err := telemetry.SupplementTraceExportersFromEnv(
			exporters,
			func(key string) (string, bool) {
				return fmt.Sprintf("%v,%v", telemetry.OtelTracesOtlpExporterType, "nonsense"), true
			})

		require.EqualError(t, err, "unsupported OTEL env: OTEL_TRACES_EXPORTER=nonsense")
		require.Empty(t, exporters)
	})

	t.Run("when not specified, do not create any exporters", func(t *testing.T) {
		exporters := map[telemetry.SpanExporterType]otelsdktrace.SpanExporter{}

		err := telemetry.SupplementTraceExportersFromEnv(
			exporters,
			func(key string) (string, bool) {
				return "", false
			})

		require.NoError(t, err)
		require.Empty(t, exporters)
	})
}

func TestResourceServiceName(t *testing.T) {
	t.Run("when env variable is specified, use custom service name prefix", func(t *testing.T) {
		require.Equal(t,
			"PREFIX.matching",
			telemetry.ResourceServiceName(primitives.MatchingService, func(key string) (string, bool) {
				require.Equal(t, telemetry.OtelServiceNameEnvKey, key)
				return "PREFIX", true
			}),
		)
	})

	t.Run("when not specified, use default prefix", func(t *testing.T) {
		require.Equal(t,
			"io.temporal.history",
			telemetry.ResourceServiceName(primitives.HistoryService, func(key string) (string, bool) {
				return "", false
			}),
		)
	})

	t.Run("always use single service name for internal frontend", func(t *testing.T) {
		require.Equal(t,
			"PREFIX.frontend",
			telemetry.ResourceServiceName(primitives.InternalFrontendService, func(key string) (string, bool) {
				return "PREFIX", true
			}),
		)
		require.Equal(t,
			"io.temporal.frontend",
			telemetry.ResourceServiceName(primitives.InternalFrontendService, func(key string) (string, bool) {
				return "", false
			}),
		)
	})
}
