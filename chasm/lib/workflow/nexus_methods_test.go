package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// scheduleTestNexusOperation schedules a Nexus operation on the test workflow and returns the
// scheduled operation together with its scheduled event ID.
func scheduleTestNexusOperation(t *testing.T, tcx testContext) *nexusoperation.Operation {
	t.Helper()
	err := tcx.scheduleHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
		Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
			ScheduleNexusOperationCommandAttributes: &commandpb.ScheduleNexusOperationCommandAttributes{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "op",
			},
		},
	}, CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
	require.NoError(t, err)
	require.Len(t, tcx.history.Events, 1)

	event := tcx.history.Events[0]
	opField, ok := tcx.wf.Operations[event.EventId]
	require.True(t, ok)
	return opField.Get(tcx.chasmCtx)
}

// TestOnNexusOperationCompletion_UsesCloseTime verifies that the completion/failure/cancellation
// history events stamp the callback-reported close time when provided, and fall back to the
// current time otherwise.
func TestOnNexusOperationCompletion_UsesCloseTime(t *testing.T) {
	closeTime := time.Date(2020, 6, 1, 12, 0, 0, 0, time.UTC)
	fixedNow := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

	failure := &failurepb.Failure{Message: "oops"}

	for _, tc := range []struct {
		name   string
		invoke func(tcx testContext, op *nexusoperation.Operation, closeTime *time.Time) error
	}{
		{
			name: "Completed",
			invoke: func(tcx testContext, op *nexusoperation.Operation, closeTime *time.Time) error {
				return tcx.wf.OnNexusOperationCompleted(tcx.chasmCtx, op, nil, closeTime, nil)
			},
		},
		{
			name: "Failed",
			invoke: func(tcx testContext, op *nexusoperation.Operation, closeTime *time.Time) error {
				return tcx.wf.OnNexusOperationFailed(tcx.chasmCtx, op, failure, closeTime)
			},
		},
		{
			name: "Canceled",
			invoke: func(tcx testContext, op *nexusoperation.Operation, closeTime *time.Time) error {
				return tcx.wf.OnNexusOperationCanceled(tcx.chasmCtx, op, failure, closeTime)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("uses close time", func(t *testing.T) {
				tcx := newTestContext(t, defaultConfig)
				op := scheduleTestNexusOperation(t, tcx)
				overrideDefaultEventTime(tcx, fixedNow)

				require.NoError(t, tc.invoke(tcx, op, &closeTime))

				completionEvent := tcx.history.Events[len(tcx.history.Events)-1]
				require.Equal(t, closeTime, completionEvent.GetEventTime().AsTime())
			})

			t.Run("falls back to current time", func(t *testing.T) {
				tcx := newTestContext(t, defaultConfig)
				op := scheduleTestNexusOperation(t, tcx)
				overrideDefaultEventTime(tcx, fixedNow)

				require.NoError(t, tc.invoke(tcx, op, nil))

				completionEvent := tcx.history.Events[len(tcx.history.Events)-1]
				require.Equal(t, fixedNow, completionEvent.GetEventTime().AsTime())
			})
		})
	}
}

// overrideDefaultEventTime pins the default EventTime assigned by the backend to newly added
// history events, so fallback-to-current-time behavior can be asserted deterministically.
func overrideDefaultEventTime(tcx testContext, now time.Time) {
	nextEventID := int64(len(tcx.history.Events) + 5)
	tcx.backend.HandleAddHistoryEvent = func(eventType enumspb.EventType, setAttrs func(*historypb.HistoryEvent)) *historypb.HistoryEvent {
		e := &historypb.HistoryEvent{
			Version:   1,
			EventId:   nextEventID,
			EventTime: timestamppb.New(now),
		}
		nextEventID++
		setAttrs(e)
		tcx.history.Events = append(tcx.history.Events, e)
		return e
	}
}
