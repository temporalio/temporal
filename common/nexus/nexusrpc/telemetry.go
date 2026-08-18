package nexusrpc

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/telemetry"
)

// ServerSpanAttributes describes Nexus request attributes added to a server trace span.
type ServerSpanAttributes struct {
	Endpoint  string
	Service   string
	Operation string
	RequestID string
}

func (a ServerSpanAttributes) keyValues() []attribute.KeyValue {
	kvs := []attribute.KeyValue{
		attribute.String(telemetry.NexusServiceKey, a.Service),
		attribute.String(telemetry.NexusOperationKey, a.Operation),
	}
	if a.Endpoint != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusEndpointKey, a.Endpoint))
	}
	if a.RequestID != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusRequestIDKey, a.RequestID))
	}
	return kvs
}

// AnnotateServerSpan adds Nexus request attributes to span.
func AnnotateServerSpan(span trace.Span, attrs ServerSpanAttributes) {
	span.SetAttributes(attrs.keyValues()...)
}

// AnnotateClientRequest adds Nexus attributes to the HTTP client span created for req.
func AnnotateClientRequest(req *http.Request, targetNamespaceName string) {
	attrs := make([]attribute.KeyValue, 0, 2)
	if targetNamespaceName != "" {
		attrs = append(attrs, attribute.String(telemetry.NexusNamespaceKey, targetNamespaceName))
	}
	if requestID := req.Header.Get(headerRequestID); requestID != "" {
		attrs = append(attrs, attribute.String(telemetry.NexusRequestIDKey, requestID))
	}
	if len(attrs) > 0 {
		telemetry.SetHTTPClientSpanAttributes(req, attrs...)
	}
}
