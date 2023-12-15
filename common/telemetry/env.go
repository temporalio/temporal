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

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/server/common/primitives"
)

const (
	OtelTracesExporterEnvKey         = "OTEL_TRACES_EXPORTER"
	OtelTracesExporterProtocolEnvKey = "OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"
	OtelServiceNameEnvKey            = "OTEL_SERVICE_NAME"
)

var unsupportedEnvVar = errors.New("unsupported OpenTelemetry env var")

type envVarLookup = func(string) (string, bool)

// EnvSpanExporter creates a gRPC span exporter from environment variables, if present, as specified in
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md#configuration-options
func EnvSpanExporter(enVars envVarLookup) (otelsdktrace.SpanExporter, error) {

	typeOf, found := enVars(OtelTracesExporterEnvKey)
	if !found {
		return nil, nil
	}
	if typeOf != "oltp" {
		return nil, unsupportedEnvVarErr(OtelTracesExporterEnvKey, typeOf)
	}

	protocol, found := enVars(OtelTracesExporterProtocolEnvKey)
	if found && protocol != "grpc" {
		return nil, unsupportedEnvVarErr(OtelTracesExporterProtocolEnvKey, protocol)
	}

	return otlptracegrpc.NewUnstarted(), nil
}

// ResourceServiceName returns the OpenTelemetry tracing service name for a Temporal service.
func ResourceServiceName(rsn primitives.ServiceName, enVars envVarLookup) string {
	// map "internal-frontend" to "frontend" for the purpose of tracing
	if rsn == primitives.InternalFrontendService {
		rsn = primitives.FrontendService
	}

	// allow custom prefix via env vars
	serviceNamePrefix := "io.temporal"
	if customServicePrefix, found := enVars(OtelServiceNameEnvKey); found {
		serviceNamePrefix = customServicePrefix
	}

	return fmt.Sprintf("%s.%s", serviceNamePrefix, string(rsn))
}

func unsupportedEnvVarErr(key, val string) error {
	return fmt.Errorf("%w: %s=%s", unsupportedEnvVar, key, val)
}
