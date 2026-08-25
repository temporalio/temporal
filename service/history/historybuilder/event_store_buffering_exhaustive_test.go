package historybuilder

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

// TestEventStoreBufferEventExhaustive is a change detector for the complete public
// history-event enum. Adding an event type requires an explicit decision here instead
// of silently inheriting EventStore.bufferEvent's default-to-buffer behavior.
func TestEventStoreBufferEventExhaustive(t *testing.T) {
	bufferedEventTypes := map[enumspb.EventType]struct{}{
		enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED:                             {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED:                           {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED:                              {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT:                           {},
		enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED:                            {},
		enumspb.EVENT_TYPE_TIMER_FIRED:                                       {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED:               {},
		enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED: {},
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED:      {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED:                       {},
		enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED:             {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED:                  {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED:                {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED:                   {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED:                 {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT:                {},
		enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED:               {},
		enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED:         {},
		enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED:              {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_REJECTED:                {},
		enumspb.EVENT_TYPE_WORKFLOW_PROPERTIES_MODIFIED_EXTERNALLY:           {},
		enumspb.EVENT_TYPE_ACTIVITY_PROPERTIES_MODIFIED_EXTERNALLY:           {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ADMITTED:                {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:                           {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED:                         {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED:                            {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED:                          {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT:                         {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED:                {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED:          {},
		enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED:             {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED:                         {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UNPAUSED:                       {},
		enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIME_SKIPPING_TRANSITIONED:     {},
	}

	store := &EventStore{}
	seen := make(map[enumspb.EventType]struct{}, len(enumspb.EventType_name)-1)
	for value := range enumspb.EventType_name {
		eventType := enumspb.EventType(value)
		if eventType == enumspb.EVENT_TYPE_UNSPECIFIED {
			continue
		}
		_, expectedBuffered := bufferedEventTypes[eventType]
		require.Equal(t, expectedBuffered, store.bufferEvent(eventType), eventType.String())
		seen[eventType] = struct{}{}
	}

	require.Len(t, seen, len(enumspb.EventType_name)-1)
	for eventType := range bufferedEventTypes {
		require.Contains(t, seen, eventType, "buffered-event expectation is not a public EventType")
	}
}
