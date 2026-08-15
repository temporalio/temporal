package model

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire2/internal/fact"
)

func succeededFact(outcome string) *fact.NexusOperationSucceeded {
	f := &fact.NexusOperationSucceeded{}
	f.Outcome = outcome // promoted exported field on the embedded fact payload
	return f
}

// fireNexus routes facts to op as if they arrived for operation wf1:5.
func fireNexus(t *testing.T, op *NexusOperation, facts ...umpire.Fact) {
	t.Helper()
	ident := &umpire.EntityPath{
		EntityID:  umpire.NewEntityID(NexusOperationType, "wf1:5"),
		Ancestors: []umpire.EntityID{umpire.NewEntityID(WorkflowType, "wf1")},
	}
	seq := func(yield func(umpire.Fact) bool) {
		for _, f := range facts {
			if !yield(f) {
				return
			}
		}
	}
	require.NoError(t, op.OnFact(context.Background(), ident, seq))
}

func TestNexusOperation_LifecycleIsValidAndTotal(t *testing.T) {
	require.NoError(t, NewNexusOperation().FSM.Validate(),
		"the Nexus operation lifecycle must be structurally sound")
}

func TestNexusOperation_AsyncLifecycle(t *testing.T) {
	op := NewNexusOperation()
	fireNexus(t, op,
		&fact.NexusOperationScheduled{},
		&fact.NexusOperationStarted{},
		succeededFact("success"),
	)
	require.Equal(t, "succeeded", op.FSM.Current())
	require.True(t, op.FSM.IsTerminal())
	require.Equal(t, "success", op.Outcome)
	require.Equal(t, "wf1", op.WorkflowID, "workflow ID is derived from the parent path")
	require.False(t, op.ScheduledAt().IsZero())
	require.False(t, op.StartedAt().IsZero())
	_, settled := op.SettledAt()
	require.True(t, settled)
}

func TestNexusOperation_BackoffThenRetryThenStart(t *testing.T) {
	op := NewNexusOperation()
	fireNexus(t, op,
		&fact.NexusOperationScheduled{},
		&fact.NexusOperationAttemptFailed{}, // scheduled -> backing_off
		&fact.NexusOperationScheduled{},     // retry: backing_off -> scheduled
		&fact.NexusOperationStarted{},
	)
	require.Equal(t, "started", op.FSM.Current())
	require.True(t, op.FSM.Reached("backing_off"))
}

// The same FSM is driven by the generic CHASM transition telemetry — a real CHASM
// operation observed via chasm.transition events (destination = OperationStatus).
func TestNexusOperation_DrivenByChasmTransitions(t *testing.T) {
	op := NewNexusOperation()
	chasm := func(dest string) *fact.ChasmTransition {
		f := &fact.ChasmTransition{}
		f.ComponentType, f.RequestID, f.WorkflowID, f.Destination = "*nexusoperation.Operation", "req-5", "wf1", dest
		return f
	}
	fireNexus(t, op,
		chasm("OPERATION_STATUS_SCHEDULED"),
		chasm("OPERATION_STATUS_STARTED"),
		chasm("OPERATION_STATUS_SUCCEEDED"),
	)
	require.Equal(t, "succeeded", op.FSM.Current())
	require.True(t, op.FSM.IsTerminal())
	require.Equal(t, "OPERATION_STATUS_SUCCEEDED", op.Outcome)
	require.Equal(t, "req-5", op.ScheduledEventID) // request ID captured as the op identity
}

// Sync completion skips STARTED: scheduled -> succeeded directly.
func TestNexusOperation_SyncCompletionSkipsStarted(t *testing.T) {
	op := NewNexusOperation()
	fireNexus(t, op,
		&fact.NexusOperationScheduled{},
		succeededFact("success"),
	)
	require.Equal(t, "succeeded", op.FSM.Current())
	require.True(t, op.StartedAt().IsZero(), "STARTED must not be marked reached on sync completion")
}

func TestNexusOperation_UsesSpanEventTimes(t *testing.T) {
	scheduledAt := time.Date(2026, time.August, 12, 14, 0, 0, 0, time.UTC)
	startedAt := scheduledAt.Add(time.Second)
	settledAt := startedAt.Add(time.Second)
	scheduled := &fact.NexusOperationScheduled{}
	scheduled.SetEventTime(scheduledAt)
	started := &fact.NexusOperationStarted{}
	started.SetEventTime(startedAt)
	succeeded := succeededFact("success")
	succeeded.SetEventTime(settledAt)

	op := NewNexusOperation()
	fireNexus(t, op, scheduled, started, succeeded)

	require.Equal(t, scheduledAt, op.ScheduledAt())
	require.Equal(t, startedAt, op.StartedAt())
	actualSettledAt, ok := op.SettledAt()
	require.True(t, ok)
	require.Equal(t, settledAt, actualSettledAt)
}

func TestNexusOperationRetainsStartedHistoryReference(t *testing.T) {
	startedAt := time.Date(2026, time.August, 12, 14, 0, 0, 0, time.UTC)
	history := &fact.NexusOperationStartedHistory{
		WorkflowID:          "wf1",
		ScheduledEventID:    "5",
		StartedEventID:      "8",
		HandlerWorkflowID:   "handler-id",
		HandlerRunID:        "handler-run-id",
		ReferenceKind:       "event",
		ReferenceValue:      "1",
		ReferencedEventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
	}
	history.SetEventTime(startedAt)
	op := NewNexusOperation()
	fireNexus(t, op, &fact.NexusOperationScheduled{}, history)

	require.Equal(t, startedAt, op.StartedAt())
	require.Equal(t, startedAt, op.StartHistoryEventTime)
	require.Equal(t, "handler-id", op.HandlerWorkflowID)
	require.Equal(t, "handler-run-id", op.HandlerRunID)
	require.Equal(t, "event", op.StartReferenceKind)
	require.Equal(t, "1", op.StartReferenceValue)
	require.Equal(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED, op.StartReferencedEventType)
}

func TestNexusOperationTerminalHistoryAdvancesLifecycle(t *testing.T) {
	settledAt := time.Date(2026, time.August, 12, 15, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		kind  enumspb.EventType
		state string
	}{
		{kind: enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED, state: NexusSucceeded},
		{kind: enumspb.EVENT_TYPE_NEXUS_OPERATION_FAILED, state: NexusFailed},
		{kind: enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED, state: NexusCanceled},
		{kind: enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, state: NexusTimedOut},
	} {
		t.Run(test.kind.String(), func(t *testing.T) {
			terminal := &fact.NexusOperationTerminal{
				NamespaceID:      "namespace-id",
				WorkflowID:       "wf1",
				ScheduledEventID: "5",
				Kind:             test.kind.String(),
			}
			terminal.SetEventTime(settledAt)
			op := NewNexusOperation()
			fireNexus(t, op, &fact.NexusOperationScheduled{}, &fact.NexusOperationStarted{}, terminal)

			require.Equal(t, test.state, op.FSM.Current())
			actual, ok := op.SettledAt()
			require.True(t, ok)
			require.Equal(t, settledAt, actual)
		})
	}
}
