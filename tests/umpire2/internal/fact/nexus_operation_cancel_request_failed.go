package fact

import "go.temporal.io/server/common/testing/umpire"

// NexusOperationCancelRequestFailed is an explicitly observed failed cancellation request.
// The operation remains active; a later terminal completion may still settle it.
type NexusOperationCancelRequestFailed struct {
	NamespaceID      string
	WorkflowID       string
	ScheduledEventID string
	RequestedEventID string
	FailureMessage   string
	EntityPath       *umpire.EntityPath
}

func (*NexusOperationCancelRequestFailed) Name() string {
	return "NexusOperationCancelRequestFailed"
}

func (e *NexusOperationCancelRequestFailed) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func NewNexusOperationCancelRequestFailed(
	namespaceID string,
	workflowID string,
	scheduledEventID string,
	requestedEventID string,
	failureMessage string,
) *NexusOperationCancelRequestFailed {
	self := umpire.NewEntityID(NexusOperationType, workflowID+":"+scheduledEventID)
	parent := umpire.NewEntityID(WorkflowType, workflowID)
	return &NexusOperationCancelRequestFailed{
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		ScheduledEventID: scheduledEventID,
		RequestedEventID: requestedEventID,
		FailureMessage:   failureMessage,
		EntityPath:       nsPath(namespaceID, self, parent),
	}
}
