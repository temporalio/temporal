package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// applyCancelRequestedEvent applies a NexusOperationCancelRequested event. A nil principal is a
// user-initiated cancel; nexusoperation.SystemPrincipal() marks an auto-close (system) one.
func applyCancelRequestedEvent(
	t *testing.T,
	tcx testContext,
	scheduledEventID int64,
	eventID int64,
	systemInitiated bool,
) {
	t.Helper()
	event := &historypb.HistoryEvent{
		EventId:   eventID,
		EventTime: timestamppb.Now(),
		Attributes: &historypb.HistoryEvent_NexusOperationCancelRequestedEventAttributes{
			NexusOperationCancelRequestedEventAttributes: &historypb.NexusOperationCancelRequestedEventAttributes{
				ScheduledEventId: scheduledEventID,
			},
		},
	}
	if systemInitiated {
		event.Principal = nexusoperation.SystemPrincipal()
	}
	applyEventDefinition[CancelRequestedEventDefinition](t, tcx, event)
}

func startedOperation(t *testing.T, tcx testContext) (scheduledEventID int64) {
	t.Helper()
	scheduledEvent, key := scheduleOperation(t, tcx)
	applyStartedEvent(t, tcx, scheduledEvent.EventId, time.Now().UTC())
	require.Equal(t, nexusoperationpb.OPERATION_STATUS_STARTED, tcx.wf.Operations[key].Get(tcx.chasmCtx).Status)
	return key
}

func cancellationOf(t *testing.T, tcx testContext, key int64) *nexusoperation.Cancellation {
	t.Helper()
	op := tcx.wf.Operations[key].Get(tcx.chasmCtx)
	cancellation, ok := op.Cancellation.TryGet(tcx.chasmCtx)
	require.True(t, ok, "expected a pending cancellation")
	return cancellation
}

// countCancelRequestedEvents counts NexusOperationCancelRequested events *emitted* into history.
// Events fed through applyEventDefinition replay an existing history and are not counted.
func countCancelRequestedEvents(tcx testContext, scheduledEventID int64) int {
	n := 0
	for _, e := range tcx.history.Events {
		attrs := e.GetNexusOperationCancelRequestedEventAttributes()
		if attrs != nil && attrs.GetScheduledEventId() == scheduledEventID {
			n++
		}
	}
	return n
}

// TestOnWorkflowReset covers the reset matrix: a reset is a force-close for exactly the operations
// the reset run does not adopt, and a no-op for the ones it does — except that an adopted
// operation's pending system-initiated cancellation must be aborted, since the close that justified
// it is being undone.
func TestOnWorkflowReset(t *testing.T) {
	t.Run("dropped started operation is cancelled", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)

		// Reset point precedes the scheduled event, so the reset run never rebuilds this operation.
		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key-1, true))

		require.Equal(t, 1, countCancelRequestedEvents(tcx, key))
		protorequire.ProtoEqual(t, nexusoperation.SystemPrincipal(), cancellationOf(t, tcx, key).GetPrincipal())
	})

	t.Run("adopted started operation is left alone", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key, true))

		require.Zero(t, countCancelRequestedEvents(tcx, key))
		_, ok := tcx.wf.Operations[key].Get(tcx.chasmCtx).Cancellation.TryGet(tcx.chasmCtx)
		require.False(t, ok, "adopted operation must not be cancelled by the reset")
	})

	t.Run("dropped operation that never started is left alone", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		_, key := scheduleOperation(t, tcx)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key-1, true))

		require.Zero(t, countCancelRequestedEvents(tcx, key), "no handler to cancel before the operation starts")
	})

	t.Run("dropped started operation is left alone under ABANDON", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key-1, false))

		require.Zero(t, countCancelRequestedEvents(tcx, key))
	})

	t.Run("adopted operation's system cancellation is aborted", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)
		applyCancelRequestedEvent(t, tcx, key, key+10, true)
		require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellationOf(t, tcx, key).Status)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key, true))

		// Terminal: the detached cancellation stops retrying and cannot outlive the reset.
		require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_FAILED, cancellationOf(t, tcx, key).Status)
	})

	t.Run("adopted operation's user cancellation survives", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)
		applyCancelRequestedEvent(t, tcx, key, key+10, false)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key, true))

		// A user cancel is attached, so it stops with the closing run on its own; the reset run
		// rebuilds one iff its event falls at or before the reset point.
		require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellationOf(t, tcx, key).Status)
	})

	t.Run("dropped operation's system cancellation survives", func(t *testing.T) {
		tcx := newTestContext(t, defaultConfig)
		key := startedOperation(t, tcx)
		applyCancelRequestedEvent(t, tcx, key, key+10, true)

		require.NoError(t, tcx.wf.OnWorkflowReset(tcx.chasmCtx, key-1, true))

		// Nothing adopts this handler, so the in-flight cancel is the only thing cleaning it up.
		require.Equal(t, nexusoperationpb.CANCELLATION_STATUS_SCHEDULED, cancellationOf(t, tcx, key).Status)
		require.Zero(t, countCancelRequestedEvents(tcx, key), "must not emit a second cancel request for one operation")
	})
}
