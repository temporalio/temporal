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
	"os"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelsdktrace "go.opentelemetry.io/otel/sdk/trace"
	"gopkg.in/errgo.v2/fmt/errors"
)

// EnvSpanExporter creates gRPC span exporters from environment variables as specified in
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md#configuration-options
func EnvSpanExporter() (otelsdktrace.SpanExporter, error) {
	typeOf, found := os.LookupEnv("OTEL_TRACES_EXPORTER")
	if !found {
		return nil, nil
	}
	if typeOf != "oltp" {
		return nil, errors.Newf("unsupported OTEL_TRACES_EXPORTER: %v", typeOf)
	}

	protocol, found := os.LookupEnv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
	if found && protocol != "grpc" {
		return nil, errors.Newf("unsupported OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: %v", protocol)
	}

	return otlptracegrpc.NewUnstarted(), nil
}
