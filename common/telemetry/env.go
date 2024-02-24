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
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/primitives"
	"golang.org/x/exp/maps"
)

const (
	OtelServiceNameEnvKey      = "OTEL_SERVICE_NAME"
	OtelTracesExporterEnvKey   = "OTEL_TRACES_EXPORTER"
	OtelTracesOtelExporterType = SpanExporterType("otlp")
)

type envVarLookup = func(string) (string, bool)

// SupplementTraceExportersFromEnv adds any OTEL span exporters configured through environment variables
// to a given set of exporters. It does not override an existing exporter with the same type.
func SupplementTraceExportersFromEnv(
	exporters map[SpanExporterType]otelsdktrace.SpanExporter,
	envVars envVarLookup,
) error {
	supplements := map[SpanExporterType]otelsdktrace.SpanExporter{}
	if envVal, ok := envVars(OtelTracesExporterEnvKey); ok {
		for _, val := range strings.Split(envVal, ",") {
			switch SpanExporterType(val) {
			case OtelTracesOtelExporterType:
				if _, exists := exporters[OtelTracesOtelExporterType]; !exists {
					// other OTEL configuration env variables are picked up automatically by the exporter itself
					supplements[OtelTracesOtelExporterType] = otlptracegrpc.NewUnstarted()
				}
			default:
				return fmt.Errorf("unsupported %v: %v", OtelTracesExporterEnvKey, val)
			}
		}
	}
	maps.Copy(exporters, supplements)
	return nil
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
