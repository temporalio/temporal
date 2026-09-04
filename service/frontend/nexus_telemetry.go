package frontend

import (
	"context"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

func (c *operationContext) annotateServerSpan(
	ctx context.Context,
	service, operation, requestID string,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	nexusrpc.AnnotateServerSpan(span, nexusrpc.ServerSpanAttributes{
		Endpoint:  c.endpointName,
		Service:   service,
		Operation: operation,
		RequestID: requestID,
	})
}

func (c *operationContext) annotateServerSpanLinks(
	ctx context.Context,
	links []nexus.Link,
) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}
	nexusrpc.AnnotateServerSpanLinks(span, links)
}
