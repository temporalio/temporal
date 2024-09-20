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

	for _, exporterType := range strings.Split(exporterTypes, ",") {
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
