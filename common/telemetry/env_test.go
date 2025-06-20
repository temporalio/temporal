package telemetry_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/telemetry"
)

func TestSupplementTraceExportersFromEnv(t *testing.T) {
	t.Run("when env variable specifies valid OTEL exporter type, add exporter", func(t *testing.T) {
		exporters, err := telemetry.SpanExportersFromEnv(
			func(key string) (string, bool) {
				if key == telemetry.OtelTracesExporterTypesEnvKey {
					return string(telemetry.OtelTracesOtlpExporterType), true
				}
				return "", false
			})

		require.NoError(t, err)
		require.Len(t, exporters, 1)
	})

	t.Run("when env variable specifies valid OTEL exporter type but invalid protocol, return error", func(t *testing.T) {
		exporters, err := telemetry.SpanExportersFromEnv(
			func(key string) (string, bool) {
				switch key {
				case telemetry.OtelTracesExporterTypesEnvKey:
					return string(telemetry.OtelTracesOtlpExporterType), true
				case telemetry.OtelExporterOtlpTracesProtocolEnvKey:
					return "invalid", true
				}
				return "", false
			})

		require.EqualError(t, err, "unsupported OTEL exporter protocol: OTEL_EXPORTER_OTLP_TRACES_PROTOCOL=invalid")
		require.Empty(t, exporters)
	})

	t.Run("when env variable is specified but exporter type is not supported, return error", func(t *testing.T) {
		exporters, err := telemetry.SpanExportersFromEnv(
			func(key string) (string, bool) {
				if key == telemetry.OtelTracesExporterTypesEnvKey {
					return fmt.Sprintf("%v,%v", telemetry.OtelTracesOtlpExporterType, "nonsense"), true
				}
				return "", false
			})

		require.EqualError(t, err, "unsupported OTEL exporter: OTEL_TRACES_EXPORTER=nonsense")
		require.Empty(t, exporters)
	})

	t.Run("when not specified, do not create any exporters", func(t *testing.T) {
		exporters, err := telemetry.SpanExportersFromEnv(
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
