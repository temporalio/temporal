package nexusrpc

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/telemetry"
)

// SpanAttributes describes Nexus request attributes added to a trace span.
type SpanAttributes struct {
	Request             bool
	NamespaceName       string
	TargetNamespaceName string
	Endpoint            string
	Service             string
	Operation           string
	RequestID           string
}

func (a SpanAttributes) keyValues() []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, 0, 7)
	if a.Request {
		kvs = append(kvs, attribute.Bool(telemetry.NexusRequestKey, true))
	}
	if a.NamespaceName != "" {
		kvs = append(kvs, attribute.String(telemetry.NamespaceKey, a.NamespaceName))
	}
	if a.TargetNamespaceName != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusNamespaceKey, a.TargetNamespaceName))
	}
	if a.Endpoint != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusEndpointKey, a.Endpoint))
	}
	if a.Service != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusServiceKey, a.Service))
	}
	if a.Operation != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusOperationKey, a.Operation))
	}
	if a.RequestID != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusRequestIDKey, a.RequestID))
	}
	return kvs
}

// SetSpanAttributes adds Nexus request attributes to span.
func SetSpanAttributes(span trace.Span, attrs SpanAttributes) {
	if kvs := attrs.keyValues(); len(kvs) > 0 {
		span.SetAttributes(kvs...)
	}
}

// MarkHTTPRequest adds Nexus attributes to the HTTP client span created for req.
func MarkHTTPRequest(req *http.Request, namespaceName string, targetNamespaceName string) {
	telemetry.SetHTTPClientSpanAttributes(req, SpanAttributes{
		Request:             true,
		NamespaceName:       namespaceName,
		TargetNamespaceName: targetNamespaceName,
		RequestID:           req.Header.Get(HeaderRequestID),
	}.keyValues()...)
}
