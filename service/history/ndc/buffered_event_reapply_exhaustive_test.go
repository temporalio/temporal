package ndc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/service/history/hsm"
)

// TestBufferedEventConflictDispositionExhaustive documents the conflict outcome
// for every event that EventStore can buffer. Keep this table in lockstep with
// historybuilder.TestEventStoreBufferEventExhaustive: that test guards the
// buffering decision, while this one guards the losing-branch/reapply decision.
func TestBufferedEventConflictDispositionExhaustive(t *testing.T) {
	alwaysReapplied := eventTypeSet(
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
	)
	childReappliedWhenInitiatedEventExists := eventTypeSet(
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT,
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED,
	)
	nexusReappliedWhenOperationExists := eventTypeSet(
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT,
	)
	nexusNeverReapplied := eventTypeSet(
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED,
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED,
	)
	losingBranchOnly := eventTypeSet(
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT,
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED,
		enumspb.EVENT_TYPE_TIMER_FIRED,
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED,
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED,
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED,
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY,
		enumspb.EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED,
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED,
	)

	dispositions := []map[enumspb.EventType]struct{}{
		alwaysReapplied,
		childReappliedWhenInitiatedEventExists,
		nexusReappliedWhenOperationExists,
		nexusNeverReapplied,
		losingBranchOnly,
	}
	allBuffered := make(map[enumspb.EventType]struct{})
	for _, disposition := range dispositions {
		for eventType := range disposition {
			_, duplicate := allBuffered[eventType]
			require.False(t, duplicate, "%s has more than one conflict disposition", eventType)
			allBuffered[eventType] = struct{}{}
		}
	}
	require.Len(t, allBuffered, 34)

	// Prove that every event classified as losing-branch-only actually traverses
	// the generic dispatcher without being appended to the winning branch.
	for eventType := range losingBranchOnly {
		t.Run(eventType.String(), func(t *testing.T) {
			reapplied, err := reapplyEvents(
				context.Background(),
				nil,
				nil,
				hsm.NewRegistry(),
				chasmworkflow.NewRegistry(),
				[]*historypb.HistoryEvent{{EventType: eventType}},
				nil,
				"",
				false,
				log.NewNoopLogger(),
			)
			require.NoError(t, err)
			require.Empty(t, reapplied)
		})
	}
}

func eventTypeSet(eventTypes ...enumspb.EventType) map[enumspb.EventType]struct{} {
	result := make(map[enumspb.EventType]struct{}, len(eventTypes))
	for _, eventType := range eventTypes {
		result[eventType] = struct{}{}
	}
	return result
}
