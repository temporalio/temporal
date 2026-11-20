package nexusrpc

import (
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/temporalnexus"
)

func TestAnnotateServerSpanLinks_IgnoresUnrelatedLinks(t *testing.T) {
	for _, tc := range []struct {
		name  string
		event *commonpb.Link_WorkflowEvent
	}{
		{
			name: "workflow completion event",
			event: &commonpb.Link_WorkflowEvent{
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
					},
				},
			},
		},
		{
			name: "different request ID",
			event: &commonpb.Link_WorkflowEvent{
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: "other-request-id",
					},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := tracetest.NewSpanRecorder()
			tracerProvider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
			_, span := tracerProvider.Tracer("test").Start(t.Context(), "test")

			tc.event.Namespace = "namespace"
			tc.event.WorkflowId = "workflow-id"
			tc.event.RunId = "run-id"
			link := temporalnexus.ConvertLinkWorkflowEventToNexusLink(tc.event)
			AnnotateServerSpanLinks(span, "request-id", []nexus.Link{link})
			span.End()

			spans := recorder.Ended()
			require.Len(t, spans, 1)
			require.Empty(t, spans[0].Links())
			require.Empty(t, spans[0].Attributes())
		})
	}
}
