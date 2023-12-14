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
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/telemetry"
)

func TestEnvSpanExporter(t *testing.T) {
	exp, err := telemetry.EnvSpanExporter()
	require.Nil(t, exp)
	require.NoError(t, err)

	os.Setenv("OTEL_TRACES_EXPORTER", "invalid-exporter")

	exp, err = telemetry.EnvSpanExporter()
	require.Nil(t, exp)
	require.EqualError(t, err, "unsupported OTEL_TRACES_EXPORTER: invalid-exporter")

	os.Setenv("OTEL_TRACES_EXPORTER", "oltp")

	exp, err = telemetry.EnvSpanExporter()
	require.NotNil(t, exp)
	require.NoError(t, err)

	os.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")

	exp, err = telemetry.EnvSpanExporter()
	require.NotNil(t, exp)
	require.NoError(t, err)

	os.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "grpc")

	exp, err = telemetry.EnvSpanExporter()
	require.NotNil(t, exp)
	require.NoError(t, err)

	os.Setenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL", "invalid-protocol")

	exp, err = telemetry.EnvSpanExporter()
	require.Nil(t, exp)
	require.EqualError(t, err, "unsupported OTEL_EXPORTER_OTLP_TRACES_PROTOCOL: invalid-protocol")
}
