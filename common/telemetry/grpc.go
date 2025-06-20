package telemetry

import (
	"context"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	otelnoop "go.opentelemetry.io/otel/trace/noop"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc/interceptor/logtags"
	"go.temporal.io/server/common/tasktoken"
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

func newCustomServerStatsHandler(
	handler stats.Handler,
	logger log.Logger,
) *customServerStatsHandler {
	return &customServerStatsHandler{
		wrapped: handler,
		isDebug: DebugMode(),
		tags:    logtags.NewWorkflowTags(tasktoken.NewSerializer(), logger),
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
	case *stats.InHeader:
		if c.isDebug {
			span := trace.SpanFromContext(ctx)
			for key, values := range s.Header {
				span.SetAttributes(attribute.StringSlice("rpc.request.headers."+key, values))
			}
			if deadline, ok := ctx.Deadline(); ok {
				span.SetAttributes(attribute.String("rpc.request.deadline", deadline.Format(time.RFC3339Nano)))
				span.SetAttributes(attribute.String("rpc.request.timeout", time.Until(deadline).String()))
			}
		}
	case *stats.InPayload:
		span := trace.SpanFromContext(ctx)
		c.annotateTags(ctx, span, s.Payload)

		// annotate with gRPC request payload
		if c.isDebug {
			//revive:disable-next-line:unchecked-type-assertion
			reqMsg := s.Payload.(proto.Message)
			payload, _ := protojson.Marshal(reqMsg)
			msgType := string(proto.MessageName(reqMsg).Name())
			span.SetAttributes(attribute.Key("rpc.request.payload").String(string(payload)))
			span.SetAttributes(attribute.Key("rpc.request.type").String(msgType))
		}
	case *stats.OutHeader:
		if c.isDebug {
			span := trace.SpanFromContext(ctx)
			for key, values := range s.Header {
				span.SetAttributes(attribute.StringSlice("rpc.response.headers."+key, values))
			}
		}
	case *stats.OutPayload:
		span := trace.SpanFromContext(ctx)
		c.annotateTags(ctx, span, s.Payload)

		// annotate with gRPC response payload
		if c.isDebug {
			//revive:disable-next-line:unchecked-type-assertion
			respMsg := s.Payload.(proto.Message)
			payload, _ := protojson.Marshal(respMsg)
			msgType := string(proto.MessageName(respMsg).Name())
			span.SetAttributes(attribute.Key("rpc.response.payload").String(string(payload)))
			span.SetAttributes(attribute.Key("rpc.response.type").String(msgType))
		}
	}
}

func (c *customServerStatsHandler) annotateTags(
	ctx context.Context,
	span trace.Span,
	payload any,
) {
	methodName, ok := ctx.Value(methodNameKey{}).(string)
	if !ok {
		methodName = "unknown"
	}

	// annotate span with workflow tags (same ones the Temporal SDKs use)
	for _, logTag := range c.tags.Extract(payload, methodName) {
		var k string
		switch logTag.Key() {
		case tag.WorkflowIDKey:
			k = WorkflowIDKey
		case tag.WorkflowRunIDKey:
			k = WorkflowRunIDKey
		default:
			continue
		}
		span.SetAttributes(attribute.Key(k).String(logTag.Value().(string)))
	}
}

func (c *customServerStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return c.wrapped.TagConn(ctx, info)
}

func (c *customServerStatsHandler) HandleConn(ctx context.Context, stat stats.ConnStats) {
	c.wrapped.HandleConn(ctx, stat)
}

func isEnabled(tp trace.TracerProvider) bool {
	_, isNoop := tp.(otelnoop.TracerProvider)
	return !isNoop
}
