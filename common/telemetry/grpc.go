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
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

type (
	// ServerTraceInterceptor gives a named type to the
	// grpc.UnaryServerInterceptor implementation provided by otelgrpc
	ServerTraceInterceptor grpc.UnaryServerInterceptor

	// ClientTraceInterceptor gives a named type to the
	// grpc.UnaryClientInterceptor implementation provided by otelgrpc
	ClientTraceInterceptor grpc.UnaryClientInterceptor
)

// NewServerTraceInterceptor creates a new gRPC server interceptor that tracks
// each request with an encapsulating span using the provided TracerProvider and
// TextMapPropagator.
func NewServerTraceInterceptor(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
) ServerTraceInterceptor {
	return ServerTraceInterceptor(
		otelgrpc.UnaryServerInterceptor(
			otelgrpc.WithPropagators(tmp),
			otelgrpc.WithTracerProvider(tp),
		),
	)
}

// NewClientTraceInterceptor creates a new gRPC client interceptor that tracks
// each request with an encapsulating span using the provided TracerProvider and
// TextMapPropagator.
func NewClientTraceInterceptor(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
) ClientTraceInterceptor {
	return ClientTraceInterceptor(
		otelgrpc.UnaryClientInterceptor(
			otelgrpc.WithPropagators(tmp),
			otelgrpc.WithTracerProvider(tp),
		),
	)
}
