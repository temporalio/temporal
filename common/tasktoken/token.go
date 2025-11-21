package tasktoken

import (
	clockspb "go.temporal.io/server/api/clock/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func NewWorkflowTaskToken(
	namespaceID string,
	workflowID string,
	runID string,
	scheduledEventID int64,
	startedEventId int64,
	startedTime *timestamppb.Timestamp,
	attempt int32,
	clock *clockspb.VectorClock,
	version int64,
) *tokenspb.Task {
	return &tokenspb.Task{
		NamespaceId:      namespaceID,
		WorkflowId:       workflowID,
		RunId:            runID,
		ScheduledEventId: scheduledEventID,
		StartedEventId:   startedEventId,
		StartedTime:      startedTime,
		Attempt:          attempt,
		Clock:            clock,
		Version:          version,
	}
}

func NewActivityTaskToken(
	namespaceID string,
	workflowID string,
	runID string,
	scheduledEventID int64,
	activityId string,
	activityType string,
	attempt int32,
	clock *clockspb.VectorClock,
	version int64,
	startVersion int64,
	componentRef []byte,
) *tokenspb.Task {
	return &tokenspb.Task{
		NamespaceId:      namespaceID,
		WorkflowId:       workflowID,
		RunId:            runID,
		ScheduledEventId: scheduledEventID,
		ActivityType:     activityType,
		Attempt:          attempt,
		ActivityId:       activityId,
		Clock:            clock,
		Version:          version,
		StartVersion:     startVersion,
		ComponentRef:     componentRef,
	}
}
