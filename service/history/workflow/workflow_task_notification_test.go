package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
)

func TestNeedsWorkflowTaskNotification(t *testing.T) {
	for _, tc := range []struct {
		name        string
		typeOfEvent enumspb.EventType
		want        bool
	}{
		{"signal", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, true},
		{"cancel request", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, true},
		{"timer", enumspb.EVENT_TYPE_TIMER_FIRED, true},
		{"activity complete", enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, true},
		{"activity failure", enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, true},
		{"activity timeout", enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, true},
		{"activity canceled", enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, true},
		{"child complete", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, true},
		{"child failure", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_FAILED, true},
		{"child timeout", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TIMED_OUT, true},
		{"child canceled", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_CANCELED, true},
		{"child terminated", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_TERMINATED, true},
		{"nexus complete", enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, true},
		{"nexus failure", enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, true},
		{"nexus timeout", enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, true},
		{"nexus canceled", enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, true},
		{"activity start", enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, false},
		{"child start", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, false},
		{"nexus start", enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, false},
		{"workflow task", enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, false},
		{"update accepted", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_UPDATE_ACCEPTED, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, needsWorkflowTaskNotification(&historypb.HistoryEvent{EventType: tc.typeOfEvent}))
		})
	}
}
