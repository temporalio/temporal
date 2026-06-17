package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	NexusRequestIDHeader = "nexus-request-id"

	AttrTemporalNamespace      = "temporal.namespace"
	AttrTemporalNexusEndpoint  = "temporal.nexus.endpoint"
	AttrTemporalNexusNamespace = "temporal.nexus.namespace"
	AttrTemporalNexusOperation = "temporal.nexus.operation"
	AttrTemporalNexusRequest   = "temporal.nexus.request"
	AttrTemporalNexusRequestID = "temporal.nexus.request_id"
	AttrTemporalNexusService   = "temporal.nexus.service"
	AttrTemporalNexusTaskQueue = "temporal.nexus.task_queue"
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
		kvs = append(kvs, attribute.Bool(AttrTemporalNexusRequest, true))
	}
	if attrs.NamespaceName != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNamespace, attrs.NamespaceName))
	}
	if attrs.TargetNamespaceName != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusNamespace, attrs.TargetNamespaceName))
	}
	if attrs.Endpoint != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusEndpoint, attrs.Endpoint))
	}
	if attrs.Service != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusService, attrs.Service))
	}
	if attrs.Operation != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusOperation, attrs.Operation))
	}
	if attrs.RequestID != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusRequestID, attrs.RequestID))
	}
	if attrs.TaskQueue != "" {
		kvs = append(kvs, attribute.String(AttrTemporalNexusTaskQueue, attrs.TaskQueue))
	}
	if len(kvs) > 0 {
		span.SetAttributes(kvs...)
	}
}
