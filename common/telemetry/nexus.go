package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	NexusRequestIDHeader = "nexus-request-id"

	TemporalNamespaceKey = "temporal.namespace"
	NexusEndpointKey     = "temporal.nexus.endpoint"
	NexusNamespaceKey    = "temporal.nexus.namespace"
	NexusOperationKey    = "temporal.nexus.operation"
	NexusRequestKey      = "temporal.nexus.request"
	NexusRequestIDKey    = "temporal.nexus.request_id"
	NexusServiceKey      = "temporal.nexus.service"
	NexusTaskQueueKey    = "temporal.nexus.task_queue"
)

type NexusSpanAttributes struct {
	Request             bool
	NamespaceName       string
	TargetNamespaceName string
	Endpoint            string
	Service             string
	Operation           string
	RequestID           string
	TaskQueue           string
}

func AnnotateNexusSpan(ctx context.Context, attrs NexusSpanAttributes) {
	SetNexusSpanAttributes(trace.SpanFromContext(ctx), attrs)
}

func SetNexusSpanAttributes(span trace.Span, attrs NexusSpanAttributes) {
	kvs := make([]attribute.KeyValue, 0, 8)
	if attrs.Request {
		kvs = append(kvs, attribute.Bool(NexusRequestKey, true))
	}
	if attrs.NamespaceName != "" {
		kvs = append(kvs, attribute.String(TemporalNamespaceKey, attrs.NamespaceName))
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
	if attrs.TaskQueue != "" {
		kvs = append(kvs, attribute.String(NexusTaskQueueKey, attrs.TaskQueue))
	}
	if len(kvs) > 0 {
		span.SetAttributes(kvs...)
	}
}
