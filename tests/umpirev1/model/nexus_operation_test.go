package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpirev1/fact"
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
