package fact

import (
	"strconv"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// NexusOperationStartedHistory carries public start-event time and workflow-reference evidence.
type NexusOperationStartedHistory struct {
	EventTimeCarrier
	NamespaceID         string
	WorkflowID          string
	ScheduledEventID    string
	StartedEventID      string
	HandlerWorkflowID   string
	HandlerRunID        string
	ReferenceKind       string
	ReferenceValue      string
	ReferencedEventType enumspb.EventType
	Malformed           bool
	ErrorClass          string
	EntityPath          *umpire.EntityPath
}

func (*NexusOperationStartedHistory) Name() string                       { return "NexusOperationStartedHistory" }
func (e *NexusOperationStartedHistory) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NewNexusOperationStartedHistory normalizes a public NexusOperationStarted history event.
func NewNexusOperationStartedHistory(namespaceID, workflowID string, event *historypb.HistoryEvent) *NexusOperationStartedHistory {
	if event == nil || event.GetNexusOperationStartedEventAttributes() == nil {
		return nil
	}
	attributes := event.GetNexusOperationStartedEventAttributes()
	if workflowID == "" || attributes.GetScheduledEventId() == 0 {
		return nil
	}
	observed := &NexusOperationStartedHistory{
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		ScheduledEventID: strconv.FormatInt(attributes.GetScheduledEventId(), 10),
		StartedEventID:   strconv.FormatInt(event.GetEventId(), 10),
	}
	if event.GetEventTime() != nil {
		observed.SetEventTime(event.GetEventTime().AsTime())
	}
	for _, link := range event.GetLinks() {
		workflowEvent := link.GetWorkflowEvent()
		if workflowEvent == nil {
			continue
		}
		if observed.HandlerRunID != "" {
			observed.Malformed = true
			observed.ErrorClass = "ambiguous-start-reference"
			continue
		}
		observed.HandlerWorkflowID = workflowEvent.GetWorkflowId()
		observed.HandlerRunID = workflowEvent.GetRunId()
		observed.ReferenceKind, observed.ReferenceValue, observed.ReferencedEventType = workflowEventReference(workflowEvent)
	}
	if observed.HandlerWorkflowID == "" || observed.HandlerRunID == "" || observed.ReferenceKind == "" || observed.ReferenceValue == "" || observed.ReferencedEventType == enumspb.EVENT_TYPE_UNSPECIFIED {
		observed.Malformed = true
		if observed.ErrorClass == "" {
			observed.ErrorClass = "invalid-start-reference"
		}
	}
	self := umpire.NewEntityID(NexusOperationType, workflowID+":"+observed.ScheduledEventID)
	observed.EntityPath = nsPath(namespaceID, self, umpire.NewEntityID(WorkflowType, workflowID))
	return observed
}

func workflowEventReference(workflowEvent *commonpb.Link_WorkflowEvent) (kind string, value string, eventType enumspb.EventType) {
	if eventReference := workflowEvent.GetEventRef(); eventReference != nil {
		return "event", strconv.FormatInt(eventReference.GetEventId(), 10), eventReference.GetEventType()
	}
	if requestReference := workflowEvent.GetRequestIdRef(); requestReference != nil {
		return "request", requestReference.GetRequestId(), requestReference.GetEventType()
	}
	return "", "", enumspb.EVENT_TYPE_UNSPECIFIED
}
