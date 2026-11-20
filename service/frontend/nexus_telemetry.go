package frontend

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func (c *operationContext) annotateServerSpan(
	ctx context.Context,
	service, operation, requestID string,
) {
	nexusrpc.AnnotateServerSpan(trace.SpanFromContext(ctx), nexusrpc.ServerSpanAttributes{
		Endpoint:  c.endpointName,
		Service:   service,
		Operation: operation,
		RequestID: requestID,
	})
}
