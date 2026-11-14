package nexusrpc

import (
	"net/http"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/temporalnexus"
	"go.temporal.io/server/common/telemetry"
)

// ServerSpanAttributes describes Nexus request attributes added to a server trace span.
type ServerSpanAttributes struct {
	Endpoint  string
	Service   string
	Operation string
	RequestID string
}

// AnnotateServerSpanLinks adds handler workflow links from a Nexus response to span.
func AnnotateServerSpanLinks(
	span trace.Span,
	requestID string,
	links []nexus.Link,
) {
	// Non-recording spans discard attributes, so avoid constructing them.
	if !span.IsRecording() {
		return
	}
	handlerWorkflows := make(map[handlerWorkflowIdentity]struct{})
	var handlerWorkflow handlerWorkflowIdentity
	for _, link := range links {
		workflowEvent, err := temporalnexus.ConvertNexusLinkToLinkWorkflowEvent(link)
		if err != nil || !isHandlerWorkflowLink(workflowEvent, requestID) {
			continue
		}
		identity := handlerWorkflowIdentity{
			namespace:  workflowEvent.GetNamespace(),
			workflowID: workflowEvent.GetWorkflowId(),
			runID:      workflowEvent.GetRunId(),
		}
		if _, duplicate := handlerWorkflows[identity]; duplicate {
			continue
		}
		handlerWorkflows[identity] = struct{}{}
		handlerWorkflow = identity
		span.AddLink(trace.Link{Attributes: []attribute.KeyValue{
			attribute.String(telemetry.NamespaceKey, identity.namespace),
			attribute.String(telemetry.RunIDKey, identity.runID),
			attribute.String(telemetry.WorkflowIDKey, identity.workflowID),
		}})
	}
	if len(handlerWorkflows) == 1 {
		span.SetAttributes(
			attribute.String(telemetry.NexusHandlerNamespaceKey, handlerWorkflow.namespace),
			attribute.String(telemetry.NexusHandlerRunIDKey, handlerWorkflow.runID),
			attribute.String(telemetry.NexusHandlerWorkflowIDKey, handlerWorkflow.workflowID),
		)
	}
}

func isHandlerWorkflowLink(workflowEvent *commonpb.Link_WorkflowEvent, requestID string) bool {
	if workflowEvent.GetWorkflowId() == "" {
		return false
	}
	if workflowEvent.GetEventRef().GetEventType() == enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED {
		return true
	}
	return requestID != "" && workflowEvent.GetRequestIdRef().GetRequestId() == requestID
}

type handlerWorkflowIdentity struct {
	namespace  string
	workflowID string
	runID      string
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
