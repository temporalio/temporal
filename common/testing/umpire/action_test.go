package umpire_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

func TestDriveEmitsActionWindowsRejectionAndEndpointVerdict(t *testing.T) {
	rc := &observingRealizeContext{bindings: map[string]string{"operation": "operation-id"}}
	oracle := staticStateOracle{state: "completed"}
	plan := []umpire.Action{
		{Name: "handler", Kind: umpire.HandlerResponse, Realize: executionRealizer{}},
		{Name: "reject", Kind: umpire.ClientRPC, Realize: executionRealizer{fireErr: errors.New("rejected")}, Reject: &umpire.Reject{}},
		{
			Name:    "complete",
			Kind:    umpire.ClientRPC,
			Realize: executionRealizer{},
			Effects: []umpire.Effect{{Ref: umpire.Ref{Type: "NexusOperation", Var: "operation"}, Event: "complete"}},
		},
	}

	err := umpire.Drive(t.Context(), rc, oracle, staticEffectResolver{}, time.Millisecond, plan)
	require.NoError(t, err)
	require.Equal(t, []umpire.ExecutionObservation{
		{Kind: umpire.ExecutionActionStart, Action: "handler", Phase: "install", Outcome: umpire.ExecutionOutcomeStarted},
		{Kind: umpire.ExecutionActionStart, Action: "reject", Phase: "install", Outcome: umpire.ExecutionOutcomeStarted},
		{Kind: umpire.ExecutionActionStart, Action: "complete", Phase: "install", Outcome: umpire.ExecutionOutcomeStarted},
		{Kind: umpire.ExecutionActionFinish, Action: "handler", Phase: "fire", Outcome: umpire.ExecutionOutcomeSucceeded},
		{Kind: umpire.ExecutionActionFinish, Action: "reject", Phase: "fire", Outcome: umpire.ExecutionOutcomeRejected, ErrorClass: "error"},
		{Kind: umpire.ExecutionActionFinish, Action: "complete", Phase: "fire", Outcome: umpire.ExecutionOutcomeSucceeded},
		{Kind: umpire.ExecutionVerdict, Action: "complete", Checkpoint: "endpoint", Pass: true},
	}, rc.observations)
}

func TestDriveRemainsCompatibleWithoutExecutionObserver(t *testing.T) {
	rc := &plainRealizeContext{bindings: map[string]string{}}
	require.NoError(t, umpire.Drive(t.Context(), rc, staticStateOracle{}, nil, time.Millisecond, []umpire.Action{{
		Name: "action", Kind: umpire.ClientRPC, Realize: executionRealizer{},
	}}))
}

func TestDrivePropagatesExecutionObserverError(t *testing.T) {
	observerErr := errors.New("observer failed")
	rc := &observingRealizeContext{bindings: map[string]string{}, observeErr: observerErr}
	err := umpire.Drive(t.Context(), rc, staticStateOracle{}, nil, time.Millisecond, []umpire.Action{{
		Name: "action", Kind: umpire.ClientRPC, Realize: executionRealizer{},
	}})
	require.ErrorIs(t, err, observerErr)
}

func TestExecutionErrorClassIsStable(t *testing.T) {
	require.Empty(t, umpire.ExecutionErrorClass(nil))
	require.Equal(t, "canceled", umpire.ExecutionErrorClass(context.Canceled))
	require.Equal(t, "deadline_exceeded", umpire.ExecutionErrorClass(context.DeadlineExceeded))
	require.Equal(t, "error", umpire.ExecutionErrorClass(errors.New("secret detail")))
}

type plainRealizeContext struct{ bindings map[string]string }

func (c *plainRealizeContext) Binding(name string) (string, bool) {
	value, ok := c.bindings[name]
	return value, ok
}
func (c *plainRealizeContext) Bind(name, value string) { c.bindings[name] = value }

type observingRealizeContext struct {
	bindings     map[string]string
	observations []umpire.ExecutionObservation
	observeErr   error
}

func (c *observingRealizeContext) Binding(name string) (string, bool) {
	value, ok := c.bindings[name]
	return value, ok
}
func (c *observingRealizeContext) Bind(name, value string) { c.bindings[name] = value }
func (c *observingRealizeContext) ObserveExecution(_ context.Context, observed umpire.ExecutionObservation) error {
	if c.observeErr != nil {
		return c.observeErr
	}
	c.observations = append(c.observations, observed)
	return nil
}

type executionRealizer struct{ fireErr error }

func (executionRealizer) Install(umpire.RealizeContext, umpire.Action) error { return nil }
func (r executionRealizer) Fire(context.Context, umpire.RealizeContext, umpire.Action) error {
	return r.fireErr
}

type staticStateOracle struct{ state string }

func (o staticStateOracle) Current(umpire.EntityType, string) (string, bool) {
	return o.state, o.state != ""
}

type staticEffectResolver struct{}

func (staticEffectResolver) Destination(umpire.EntityType, string) (string, bool) {
	return "completed", true
}
