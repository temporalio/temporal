package model

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/fact"
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
		f.ComponentType, f.ComponentPath, f.WorkflowID, f.Destination = "*nexusoperation.Operation", "Operations/5", "wf1", dest
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
	require.Equal(t, "Operations/5", op.ScheduledEventID) // component path captured as the op identity
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

// Tier-1 (server-free): NexusTransition is total over the whole (state × event) grid, its
// lifecycle-state part agrees with the generic Lifecycle's Classify, and it predicts a
// rejection exactly on the illegal edges. This is the SAA "model is total, validated
// statically" check (UMPIRE_PRIOR_ART.md, SAA) applied to the NexusOperation archetype.
func TestNexusTransition_TotalAndConsistentWithLifecycle(t *testing.T) {
	lc := NewNexusOperation().Lifecycle()
	cfg := NexusConfig{} // unlimited attempts → no config-dependent branch
	for _, st := range lc.States() {
		for _, ev := range lc.Events() {
			out := NexusTransition(cfg, NexusAbstract{State: st, Attempt: 1}, ev)

			lc.SetState(st)
			base := lc.Classify(ev)
			require.Equal(t, base.Kind, out.Kind, "kind must match Classify at %s/%s", st, ev)
			if out.Kind == umpire.Advance {
				require.Equal(t, base.To, out.Next.State, "advance target must match Classify at %s/%s", st, ev)
			}
			if out.Kind == umpire.Illegal {
				require.NotEmpty(t, out.Reject, "an illegal edge must predict a reject at %s/%s", st, ev)
			} else {
				require.Empty(t, out.Reject, "a legal edge must not predict a reject at %s/%s", st, ev)
			}
		}
	}
}

// The config-dependent branch: with the retry budget exhausted, a retryable failure
// settles the operation terminally instead of backing off.
func TestNexusTransition_RetryBudgetExhaustedFailsTerminally(t *testing.T) {
	out := NexusTransition(NexusConfig{MaxAttempts: 1},
		NexusAbstract{State: NexusScheduled, Attempt: 1}, NexusAttemptFailed)
	require.Equal(t, umpire.Advance, out.Kind)
	require.Equal(t, NexusFailed, out.Next.State)
	require.True(t, out.Terminal)
	require.False(t, out.BackoffArmed)
}

func TestNexusTransition_RetryBudgetRemainingBacksOff(t *testing.T) {
	out := NexusTransition(NexusConfig{MaxAttempts: 3},
		NexusAbstract{State: NexusScheduled, Attempt: 1}, NexusAttemptFailed)
	require.Equal(t, NexusBackingOff, out.Next.State)
	require.True(t, out.BackoffArmed)
	require.False(t, out.Terminal)
}

func TestNexusTransition_RescheduleIncrementsAttempt(t *testing.T) {
	out := NexusTransition(NexusConfig{},
		NexusAbstract{State: NexusBackingOff, Attempt: 1}, NexusSchedule)
	require.Equal(t, NexusScheduled, out.Next.State)
	require.Equal(t, 2, out.Next.Attempt)
	require.Equal(t, 1, out.AttemptDelta)
}

func TestNexusTransition_SettledAbsorbsEvents(t *testing.T) {
	out := NexusTransition(NexusConfig{},
		NexusAbstract{State: NexusSucceeded, Attempt: 1}, NexusStart)
	require.Equal(t, umpire.NoOp, out.Kind)
	require.True(t, out.Terminal)
}

// A mini conformance walk: step the oracle through a full retry-then-succeed route and
// confirm the abstract state (including the attempt count) evolves as predicted at every
// edge — the server-free analog of SAA's traverse() asserting Outcome equality per step.
func TestNexusTransition_RetryThenSucceedWalk(t *testing.T) {
	cfg := NexusConfig{MaxAttempts: 3}
	cur := NexusAbstract{State: NexusUnspecified}
	for _, ev := range []string{
		NexusSchedule, NexusAttemptFailed, NexusSchedule, NexusStart, NexusSucceed,
	} {
		out := NexusTransition(cfg, cur, ev)
		require.NotEqual(t, umpire.Illegal, out.Kind, "event %q unexpectedly illegal at %s", ev, cur.State)
		cur = out.Next
	}
	require.Equal(t, NexusSucceeded, cur.State)
	require.Equal(t, 2, cur.Attempt, "two schedules → two attempts")
}
