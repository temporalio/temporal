package entity

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

// This is the Tier-1 static validation of the domain lifecycles against the model,
// the analog of the SAA model's validate package: it needs no server and runs in
// milliseconds, catching spec drift before any cluster spins up. Each default
// entity's executable transition function must be structurally sound (Validate)
// and total (Classify yields a defined outcome for every reachable state × event).
func TestEntityLifecyclesAreValidAndTotal(t *testing.T) {
	entities := map[string]umpire.Lifecycled{
		"Workflow":       NewWorkflow(),
		"WorkflowTask":   NewWorkflowTask(),
		"WorkflowUpdate": NewWorkflowUpdate(),
	}

	for name, e := range entities {
		t.Run(name, func(t *testing.T) {
			spec := e.Lifecycle()
			require.NoError(t, spec.Validate(), "lifecycle spec must be structurally sound")

			// Classify must be total: every reachable state × every declared event
			// maps to exactly one of the three outcomes and never panics.
			for state := range spec.Reachable() {
				lc := freshLifecycle(t, e)
				lc.SetState(state)
				for _, ev := range lc.Events() {
					o := lc.Classify(ev)
					require.Contains(t, []umpire.TransitionKind{umpire.Advance, umpire.NoOp, umpire.Illegal}, o.Kind,
						"state=%s event=%s", state, ev)
				}
			}
		})
	}
}

// freshLifecycle returns a new Lifecycle of the same shape as e's, so mutating its
// state during the totality sweep does not disturb the shared entity.
func freshLifecycle(t *testing.T, e umpire.Lifecycled) *umpire.Lifecycle {
	t.Helper()
	switch e.(type) {
	case *Workflow:
		return NewWorkflow().Lifecycle()
	case *WorkflowTask:
		return NewWorkflowTask().Lifecycle()
	case *WorkflowUpdate:
		return NewWorkflowUpdate().Lifecycle()
	default:
		t.Fatalf("unknown entity type %T", e)
		return nil
	}
}
