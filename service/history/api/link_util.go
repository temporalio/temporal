package api

import (
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common"
)

// GenerateStartedEventRefLink builds a Link pointing to the WORKFLOW_EXECUTION_STARTED event.
// Use this for backlinks to workflow start: the started event is always EventId=1 (FirstEventID)
// and is never buffered, so a concrete EventReference is appropriate.
func GenerateStartedEventRefLink(namespace, workflowID, runID string) *commonpb.Link {
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      runID,
				Reference: &commonpb.Link_WorkflowEvent_EventRef{
					EventRef: &commonpb.Link_WorkflowEvent_EventReference{
						EventId:   common.FirstEventID,
						EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
					},
				},
			},
		},
	}
}

// GenerateRequestIDRefLink builds a Link with a RequestIdReference.
// Use this for events that are buffered at signal time (e.g. SIGNALED), where the
// concrete EventId is not yet known. The server resolves the RequestId to a real
// EventId once the buffer flushes.
func GenerateRequestIDRefLink(namespace, workflowID, runID, requestID string, eventType enumspb.EventType) *commonpb.Link {
	return &commonpb.Link{
		Variant: &commonpb.Link_WorkflowEvent_{
			WorkflowEvent: &commonpb.Link_WorkflowEvent{
				Namespace:  namespace,
				WorkflowId: workflowID,
				RunId:      runID,
				Reference: &commonpb.Link_WorkflowEvent_RequestIdRef{
					RequestIdRef: &commonpb.Link_WorkflowEvent_RequestIdReference{
						RequestId: requestID,
						EventType: eventType,
					},
				},
			},
		},
	}
}
