package planner_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/umpire2/planner"
)

// Typed, per-event realization params. A concrete Temporal driver would carry the actual
// inputs here (start payloads, completion results, fault kinds); these illustrate the
// type-switch pattern at the planner→driver boundary. They live with the driver that
// consumes them, not on the model event — the planner only ever produces the label.
type (
	initializeArgs struct{ Config string }
	finishArgs     struct{ Result string }
)

// paramDriver recovers the typed params it was handed, per event, by type-switch —
// proving Action.Params flows through Plan.RunWith and arrives well-typed.
type paramDriver struct {
	config string
	result string
}

func (d *paramDriver) Do(_ context.Context, a planner.Step) error {
	switch p := a.Params.(type) {
	case initializeArgs:
		d.config = p.Config
	case finishArgs:
		d.result = p.Result
	}
	return nil
}

// RunWith binds typed params per step; the same structural route can thus be driven with
// varied inputs (the parameterized/sweep use case), and the driver recovers them by type.
func TestRunWith_BindsTypedParamsPerEvent(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)
	require.Equal(t, [][]string{{"initialize", "finish"}}, plan.Routes)

	d := &paramDriver{}
	err = plan.RunWith(context.Background(), d, func(_ int, event string) any {
		switch event {
		case "initialize":
			return initializeArgs{Config: "cfg"}
		case "finish":
			return finishArgs{Result: "ok"}
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, "cfg", d.config)
	require.Equal(t, "ok", d.result)
}

// nilParamDriver flags any non-nil Params it receives.
type nilParamDriver struct{ sawNonNil bool }

func (d *nilParamDriver) Do(_ context.Context, a planner.Step) error {
	if a.Params != nil {
		d.sawNonNil = true
	}
	return nil
}

// Run (no binder) is the structural drive: every step gets a nil Params, and the driver
// falls back to its own inputs.
func TestRun_PassesNilParams(t *testing.T) {
	plan, err := planner.PlanTo(plannerLifecycle(), "completed", planner.Shortest, planner.Constraints{})
	require.NoError(t, err)

	d := &nilParamDriver{}
	require.NoError(t, plan.Run(context.Background(), d))
	require.False(t, d.sawNonNil, "Run must hand the driver nil Params")
}
