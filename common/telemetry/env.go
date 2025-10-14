package telemetry

import (
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/primitives"
)

var (
	unsupportedTraceExporter         = errors.New("unsupported OTEL exporter")
	unsupportedTraceExporterProtocol = errors.New("unsupported OTEL exporter protocol")
)

const (
	OtelServiceNameEnvKey                = "OTEL_SERVICE_NAME"
	OtelTracesExporterTypesEnvKey        = "OTEL_TRACES_EXPORTER"
	OtelTracesOtlpExporterType           = SpanExporterType("otlp")
	OtelExporterOtlpTracesProtocolEnvKey = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	OtelExporterOtlpTracesGrcpProtocol   = "grpc"
)

type envVarLookup = func(string) (string, bool)

// SpanExportersFromEnv creates OTEL span exporters from environment variables.
func SpanExportersFromEnv(
	envVars envVarLookup,
) (map[SpanExporterType]otelsdktrace.SpanExporter, error) {
	exporters := map[SpanExporterType]otelsdktrace.SpanExporter{}

	exporterTypes, ok := envVars(OtelTracesExporterTypesEnvKey)
	if !ok {
		return exporters, nil
	}

	for exporterType := range strings.SplitSeq(exporterTypes, ",") {
		switch SpanExporterType(exporterType) {
		case OtelTracesOtlpExporterType:
			// only grpc is supported; fail if user requests a different protocol
			if protocol, exists := envVars(OtelExporterOtlpTracesProtocolEnvKey); exists {
				isSupported := protocol == OtelExporterOtlpTracesGrcpProtocol
				if !isSupported {
					return nil, fmt.Errorf("%w: %v=%v", unsupportedTraceExporterProtocol, OtelExporterOtlpTracesProtocolEnvKey, protocol)
				}
			}

			// other OTEL configuration env variables are picked up automatically by the exporter itself
			exporters[OtelTracesOtlpExporterType] = otlptracegrpc.NewUnstarted()
		case "none":
			// ignored
		default:
			return nil, fmt.Errorf("%w: %v=%v", unsupportedTraceExporter, OtelTracesExporterTypesEnvKey, exporterType)
		}
	}

	return exporters, nil
}

// ResourceServiceName returns the OpenTelemetry tracing service name for a Temporal service.
func ResourceServiceName(
	rsn primitives.ServiceName,
	envVars envVarLookup,
) string {
	// map "internal-frontend" to "frontend" for the purpose of tracing
	if rsn == primitives.InternalFrontendService {
		rsn = primitives.FrontendService
	}

	// allow custom prefix via env vars
	serviceNamePrefix := "io.temporal"
	if customServicePrefix, found := envVars(OtelServiceNameEnvKey); found {
		serviceNamePrefix = customServicePrefix
	}

	return fmt.Sprintf("%s.%s", serviceNamePrefix, string(rsn))
}
