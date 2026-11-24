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

// AnnotateServerSpan adds Nexus request attributes to span.
func AnnotateServerSpan(span trace.Span, attrs ServerSpanAttributes) {
	// Non-recording spans discard attributes, so avoid constructing them.
	if !span.IsRecording() {
		return
	}
	kvs := []attribute.KeyValue{
		attribute.String(telemetry.NexusServiceKey, attrs.Service),
		attribute.String(telemetry.NexusOperationKey, attrs.Operation),
	}
	if attrs.Endpoint != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusEndpointKey, attrs.Endpoint))
	}
	if attrs.RequestID != "" {
		kvs = append(kvs, attribute.String(telemetry.NexusRequestIDKey, attrs.RequestID))
	}
	span.SetAttributes(kvs...)
}

// AnnotateClientRequest returns req with Nexus attributes for its HTTP client span.
func AnnotateClientRequest(req *http.Request, targetNamespaceName string) *http.Request {
	attrs := make([]attribute.KeyValue, 0, 2)
	if targetNamespaceName != "" {
		attrs = append(attrs, attribute.String(telemetry.NexusNamespaceKey, targetNamespaceName))
	}
	if requestID := req.Header.Get(headerRequestID); requestID != "" {
		attrs = append(attrs, attribute.String(telemetry.NexusRequestIDKey, requestID))
	}
	if len(attrs) > 0 {
		return telemetry.WithHTTPClientSpanAttributes(req, attrs...)
	}
	return req
}
