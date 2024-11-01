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
	"go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type methodNameKey struct{}

type (
	// ServerStatsHandler gives a named type to the stats.Handler implementation provided by otelgrpc.
	ServerStatsHandler stats.Handler

	// ClientStatsHandler gives a named type to the grpc.UnaryClientInterceptor implementation provided by otelgrpc.
	ClientStatsHandler stats.Handler

	customServerStatsHandler struct {
		isDebug bool
		wrapped stats.Handler
		tags    *logtags.WorkflowTags
	}
)

// NewServerStatsHandler creates a new gRPC stats handler that tracks each request with an encapsulating span
// using the provided TracerProvider and TextMapPropagator.
//
// NOTE: If the TracerProvider is `noop.TracerProvider`, it returns `nil`.
func NewServerStatsHandler(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
	logger log.Logger,
) ServerStatsHandler {
	if !isEnabled(tp) {
		return nil
	}

	return newCustomServerStatsHandler(
		otelgrpc.NewServerHandler(
			otelgrpc.WithPropagators(tmp),
			otelgrpc.WithTracerProvider(tp),
		),
		logger)
}

// NewClientStatsHandler creates a new gRPC stats handler that tracks each request with an encapsulating span
// using the provided TracerProvider and TextMapPropagator.
//
// NOTE: If the TracerProvider is `noop.TracerProvider`, it returns `nil`.
func NewClientStatsHandler(
	tp trace.TracerProvider,
	tmp propagation.TextMapPropagator,
) ClientStatsHandler {
	if !isEnabled(tp) {
		return nil
	}

	return otelgrpc.NewClientHandler(
		otelgrpc.WithPropagators(tmp),
		otelgrpc.WithTracerProvider(tp),
	)
}

func isEnabled(tp trace.TracerProvider) bool {
	_, isNoop := tp.(noop.TracerProvider)
	return !isNoop
}

func newCustomServerStatsHandler(
	handler stats.Handler,
	logger log.Logger,
) *customServerStatsHandler {
	return &customServerStatsHandler{
		wrapped: handler,
		isDebug: debugMode(),
		tags:    logtags.NewWorkflowTags(common.NewProtoTaskTokenSerializer(), logger),
	}
}

func (c *customServerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return c.wrapped.TagRPC(
		context.WithValue(ctx, methodNameKey{}, info.FullMethodName),
		info)
}

func (c *customServerStatsHandler) HandleRPC(ctx context.Context, stat stats.RPCStats) {
	// handling `End` before wrapped stats.Handler since it closes the span
	switch s := stat.(type) {
	case *stats.End:
		// annotate with gRPC error payload
		if c.isDebug {
			span := trace.SpanFromContext(ctx)

			//revive:disable-next-line:unchecked-type-assertion
			statusErr, ok := status.FromError(s.Error)
			if ok && statusErr != nil {
				payload, _ := protojson.Marshal(statusErr.Proto())
				span.SetAttributes(attribute.Key("rpc.response.error").String(string(payload)))
			}
		}
	}

	c.wrapped.HandleRPC(ctx, stat)

	switch s := stat.(type) {
	case *stats.InPayload:
		span := trace.SpanFromContext(ctx)

		methodName, ok := ctx.Value(methodNameKey{}).(string)
		if !ok {
			methodName = "unknown"
		}

		// annotate span with workflow tags (same ones the Temporal SDKs use)
		for _, logTag := range c.tags.Extract(s.Payload, methodName) {
			var k string
			switch logTag.Key() {
			case tag.WorkflowIDKey:
				k = "temporalWorkflowID"
			case tag.WorkflowRunIDKey:
				k = "temporalRunID"
			default:
				continue
			}
			span.SetAttributes(attribute.Key(k).String(logTag.Value().(string)))
		}

		// annotate with gRPC request payload
		if c.isDebug {
			//revive:disable-next-line:unchecked-type-assertion
			reqMsg := s.Payload.(proto.Message)
			payload, _ := protojson.Marshal(reqMsg)
			msgType := string(proto.MessageName(reqMsg).Name())
			span.SetAttributes(attribute.Key("rpc.request.payload").String(string(payload)))
			span.SetAttributes(attribute.Key("rpc.request.type").String(msgType))
		}
	case *stats.OutPayload:
		// annotate with gRPC response payload
		if c.isDebug {
			span := trace.SpanFromContext(ctx)

			//revive:disable-next-line:unchecked-type-assertion
			respMsg := s.Payload.(proto.Message)
			payload, _ := protojson.Marshal(respMsg)
			msgType := string(proto.MessageName(respMsg).Name())
			span.SetAttributes(attribute.Key("rpc.response.payload").String(string(payload)))
			span.SetAttributes(attribute.Key("rpc.response.type").String(msgType))
		}
	}
}

func (c *customServerStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return c.wrapped.TagConn(ctx, info)
}

func (c *customServerStatsHandler) HandleConn(ctx context.Context, stat stats.ConnStats) {
	c.wrapped.HandleConn(ctx, stat)
}
