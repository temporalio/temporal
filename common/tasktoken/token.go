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
	startedEventID int64,
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
		StartedEventId:   startedEventID,
		StartedTime:      startedTime,
		Attempt:          attempt,
		Clock:            clock,
		Version:          version,
	}
}

// NewStandaloneActivityTaskToken builds a task token for a standalone activity.
// For standalone activities, the activity ID is used as both the workflow ID and activity ID
// in the token. Standalone activities don't use ScheduledEventId, Clock, Version, or StartVersion.
func NewStandaloneActivityTaskToken(
	namespaceID string,
	activityID string,
	runID string,
	activityType string,
	attempt int32,
	componentRef []byte,
) *tokenspb.Task {
	return NewActivityTaskToken(
		namespaceID, activityID, runID,
		0, // scheduledEventId
		activityID, activityType, attempt,
		nil, // clock
		0,   // version
		0,   // startVersion
		componentRef,
	)
}

func NewActivityTaskToken(
	namespaceID string,
	workflowID string,
	runID string,
	scheduledEventID int64,
	activityID string,
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
		ActivityId:       activityID,
		Clock:            clock,
		Version:          version,
		StartVersion:     startVersion,
		ComponentRef:     componentRef,
	}
}
