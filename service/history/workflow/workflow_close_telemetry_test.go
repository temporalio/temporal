package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNormalizeWorkflowCloseEvent(t *testing.T) {
	eventTime := time.Date(2026, time.August, 12, 12, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name      string
		event     *historypb.HistoryEvent
		outcome   string
		successor string
	}{
		{name: "completed", event: completedCloseEvent(eventTime, "cron-run"), outcome: "completed", successor: "cron-run"},
		{name: "failed", event: failedCloseEvent(eventTime, "retry-run"), outcome: "failed", successor: "retry-run"},
		{name: "canceled", event: canceledCloseEvent(eventTime), outcome: "canceled"},
		{name: "terminated", event: terminatedCloseEvent(eventTime), outcome: "terminated"},
		{name: "timed out", event: timedOutCloseEvent(eventTime, "retry-run"), outcome: "timed_out", successor: "retry-run"},
		{name: "continued as new", event: continuedAsNewCloseEvent(eventTime, "next-run"), outcome: "continued_as_new", successor: "next-run"},
	} {
		t.Run(test.name, func(t *testing.T) {
			observed, err := NormalizeWorkflowCloseEvent(test.event)
			require.NoError(t, err)
			require.Equal(t, test.outcome, observed.Outcome)
			require.Equal(t, test.successor, observed.SuccessorRunID)
			require.Equal(t, eventTime, observed.EventTime)
		})
	}
}

func TestNormalizeWorkflowCloseEventRejectsUnsupportedAndIncompleteEvents(t *testing.T) {
	_, err := NormalizeWorkflowCloseEvent(nil)
	require.Error(t, err)

	_, err = NormalizeWorkflowCloseEvent(&historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED})
	require.Error(t, err)

	_, err = NormalizeWorkflowCloseEvent(&historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED})
	require.Error(t, err)
}

func completedCloseEvent(eventTime time.Time, successor string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
			WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{NewExecutionRunId: successor},
		},
	}
}

func failedCloseEvent(eventTime time.Time, successor string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
			WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{NewExecutionRunId: successor},
		},
	}
}

func canceledCloseEvent(eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionCanceledEventAttributes{
			WorkflowExecutionCanceledEventAttributes: &historypb.WorkflowExecutionCanceledEventAttributes{},
		},
	}
}

func terminatedCloseEvent(eventTime time.Time) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTerminatedEventAttributes{
			WorkflowExecutionTerminatedEventAttributes: &historypb.WorkflowExecutionTerminatedEventAttributes{},
		},
	}
}

func timedOutCloseEvent(eventTime time.Time, successor string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionTimedOutEventAttributes{
			WorkflowExecutionTimedOutEventAttributes: &historypb.WorkflowExecutionTimedOutEventAttributes{NewExecutionRunId: successor},
		},
	}
}

func continuedAsNewCloseEvent(eventTime time.Time, successor string) *historypb.HistoryEvent {
	return &historypb.HistoryEvent{
		EventTime: timestamppb.New(eventTime),
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: &historypb.WorkflowExecutionContinuedAsNewEventAttributes{NewExecutionRunId: successor},
		},
	}
}
