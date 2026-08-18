package nexusrpc

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/telemetry"
)

// ServerSpanAttributes describes Nexus request attributes added to a server trace span.
type ServerSpanAttributes struct {
	NamespaceName       string
	TargetNamespaceName string
	Endpoint            string
	Service             string
	Operation           string
	RequestID           string
}

func (a ServerSpanAttributes) keyValues() []attribute.KeyValue {
	kvs := make([]attribute.KeyValue, 0, 6)
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

// AnnotateServerSpan adds Nexus request attributes to span.
func AnnotateServerSpan(span trace.Span, attrs ServerSpanAttributes) {
	if kvs := attrs.keyValues(); len(kvs) > 0 {
		span.SetAttributes(kvs...)
	}
}

// AnnotateClientRequest adds Nexus attributes to the HTTP client span created for req.
func AnnotateClientRequest(req *http.Request, namespaceName string, targetNamespaceName string) {
	telemetry.SetHTTPClientSpanAttributes(req, ServerSpanAttributes{
		NamespaceName:       namespaceName,
		TargetNamespaceName: targetNamespaceName,
		RequestID:           req.Header.Get(headerRequestID),
	}.keyValues()...)
}
