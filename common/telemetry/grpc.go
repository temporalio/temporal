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
	"context"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
	//nolint:staticcheck
	otelInterceptor := otelgrpc.UnaryServerInterceptor(
		otelgrpc.WithPropagators(tmp),
		otelgrpc.WithTracerProvider(tp),
	)

	if debugMode() {
		return func(
			ctx context.Context,
			req interface{},
			info *grpc.UnaryServerInfo,
			handler grpc.UnaryHandler,
		) (any, error) {
			return otelInterceptor(ctx, req, info, debugHandler(handler))
		}
	}

	return ServerTraceInterceptor(otelInterceptor)
}

func debugHandler(origHandler grpc.UnaryHandler) grpc.UnaryHandler {
	return func(ctx context.Context, req any) (resp any, err error) {
		resp, err = origHandler(ctx, req)
		span := trace.SpanFromContext(ctx)
		if span.IsRecording() {
			//revive:disable-next-line:unchecked-type-assertion
			reqMsg := req.(proto.Message)
			payload, _ := protojson.Marshal(reqMsg)
			msgType := string(proto.MessageName(reqMsg).Name())
			span.SetAttributes(attribute.Key("rpc.request.payload").String(string(payload)))
			span.SetAttributes(attribute.Key("rpc.request.type").String(msgType))

			if err == nil {
				//revive:disable-next-line:unchecked-type-assertion
				respMsg := resp.(proto.Message)
				payload, _ = protojson.Marshal(respMsg)
				msgType = string(proto.MessageName(respMsg).Name())
				span.SetAttributes(attribute.Key("rpc.response.payload").String(string(payload)))
				span.SetAttributes(attribute.Key("rpc.response.type").String(msgType))
			}
		}
		return
	}
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
