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
func NewStandaloneActivityTaskToken(
	namespaceID string,
	activityID string,
	activityType string,
	attempt int32,
	componentRef []byte,
) *tokenspb.Task {
	return NewActivityTaskToken(
		namespaceID,
		"", // workflowId — not applicable for standalone activities
		"", // runId — not applicable for standalone activities
		0,  // scheduledEventId
		activityID,
		activityType,
		attempt,
		nil, // clock
		0,   // version
		0,   // startVersion
		componentRef,
		0, // activityAttemptStamp
	)
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
	activityAttemptStamp int32,
) *tokenspb.Task {
	return &tokenspb.Task{
		NamespaceId:          namespaceID,
		WorkflowId:           workflowID,
		RunId:                runID,
		ScheduledEventId:     scheduledEventID,
		ActivityType:         activityType,
		Attempt:              attempt,
		ActivityId:           activityId,
		Clock:                clock,
		Version:              version,
		StartVersion:         startVersion,
		ComponentRef:         componentRef,
		ActivityAttemptStamp: activityAttemptStamp,
	}
}
