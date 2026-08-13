package telemetry

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type NexusSpanAttributes struct {
	Request             bool
	NamespaceName       string
	TargetNamespaceName string
	Endpoint            string
	Service             string
	Operation           string
	RequestID           string
}

func SetNexusSpanAttributes(span trace.Span, attrs NexusSpanAttributes) {
	kvs := make([]attribute.KeyValue, 0, 7)
	if attrs.Request {
		kvs = append(kvs, attribute.Bool(NexusRequestKey, true))
	}
	if attrs.NamespaceName != "" {
		kvs = append(kvs, attribute.String(NamespaceKey, attrs.NamespaceName))
	}
	if attrs.TargetNamespaceName != "" {
		kvs = append(kvs, attribute.String(NexusNamespaceKey, attrs.TargetNamespaceName))
	}
	if attrs.Endpoint != "" {
		kvs = append(kvs, attribute.String(NexusEndpointKey, attrs.Endpoint))
	}
	if attrs.Service != "" {
		kvs = append(kvs, attribute.String(NexusServiceKey, attrs.Service))
	}
	if attrs.Operation != "" {
		kvs = append(kvs, attribute.String(NexusOperationKey, attrs.Operation))
	}
	if attrs.RequestID != "" {
		kvs = append(kvs, attribute.String(NexusRequestIDKey, attrs.RequestID))
	}
	if len(kvs) > 0 {
		span.SetAttributes(kvs...)
	}
}
