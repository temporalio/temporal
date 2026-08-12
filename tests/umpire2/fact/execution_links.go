package fact

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
)

// ActivityExecutionSnapshot carries the public lifecycle and links returned by DescribeActivityExecution.
type ActivityExecutionSnapshot struct {
	NamespaceID string
	ActivityID  string
	Status      enumspb.ActivityExecutionStatus
	Links       []*commonpb.Link
	EntityPath  *umpire.EntityPath
}

func (*ActivityExecutionSnapshot) Name() string                       { return "ActivityExecutionSnapshot" }
func (e *ActivityExecutionSnapshot) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NexusOperationExecutionSnapshot carries the public links returned by DescribeNexusOperationExecution.
type NexusOperationExecutionSnapshot struct {
	NamespaceID string
	OperationID string
	Links       []*commonpb.Link
	EntityPath  *umpire.EntityPath
}

func (*NexusOperationExecutionSnapshot) Name() string                       { return "NexusOperationExecutionSnapshot" }
func (e *NexusOperationExecutionSnapshot) TargetEntity() *umpire.EntityPath { return e.EntityPath }

// NexusOperationHistorySnapshot carries timeout configuration and terminal failure metadata observed in public history.
type NexusOperationHistorySnapshot struct {
	NamespaceID         string
	WorkflowID          string
	ScheduledEventID    string
	StartToCloseTimeout time.Duration
	TimeoutType         enumspb.TimeoutType
	TimeoutMessage      string
	EntityPath          *umpire.EntityPath
}

func (*NexusOperationHistorySnapshot) Name() string { return "NexusOperationHistorySnapshot" }
func (e *NexusOperationHistorySnapshot) TargetEntity() *umpire.EntityPath {
	return e.EntityPath
}

func NewActivityExecutionSnapshot(namespaceID, activityID string, status enumspb.ActivityExecutionStatus, links []*commonpb.Link) *ActivityExecutionSnapshot {
	self := umpire.NewEntityID(ActivityType, activityID)
	return &ActivityExecutionSnapshot{NamespaceID: namespaceID, ActivityID: activityID, Status: status, Links: links, EntityPath: nsPath(namespaceID, self)}
}

func NewNexusOperationExecutionSnapshot(namespaceID, operationID string, links []*commonpb.Link) *NexusOperationExecutionSnapshot {
	self := umpire.NewEntityID(NexusOperationType, operationID+":"+operationID)
	parent := umpire.NewEntityID(WorkflowType, operationID)
	return &NexusOperationExecutionSnapshot{NamespaceID: namespaceID, OperationID: operationID, Links: links, EntityPath: nsPath(namespaceID, self, parent)}
}

func NewNexusOperationHistorySnapshot(
	namespaceID string,
	workflowID string,
	scheduledEventID string,
	startToCloseTimeout time.Duration,
	timeoutType enumspb.TimeoutType,
	timeoutMessage string,
) *NexusOperationHistorySnapshot {
	self := umpire.NewEntityID(NexusOperationType, workflowID+":"+scheduledEventID)
	parent := umpire.NewEntityID(WorkflowType, workflowID)
	return &NexusOperationHistorySnapshot{
		NamespaceID:         namespaceID,
		WorkflowID:          workflowID,
		ScheduledEventID:    scheduledEventID,
		StartToCloseTimeout: startToCloseTimeout,
		TimeoutType:         timeoutType,
		TimeoutMessage:      timeoutMessage,
		EntityPath:          nsPath(namespaceID, self, parent),
	}
}
