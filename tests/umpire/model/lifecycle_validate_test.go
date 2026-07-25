package model

import (
	"sort"
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
		"Workflow":     NewWorkflow(),
		"WorkflowTask": NewWorkflowTask(),
	}

	for name, e := range entities {
		t.Run(name, func(t *testing.T) {
			lc := e.Lifecycle()
			require.NoError(t, lc.Validate(), "lifecycle spec must be structurally sound")

			// Classify must be total: every reachable state × every declared event
			// maps to exactly one of the three defined outcomes and never panics.
			for _, c := range lc.Cells() {
				require.Contains(t, []umpire.TransitionKind{umpire.Advance, umpire.NoOp, umpire.Illegal}, c.Kind,
					"state=%s event=%s", c.From, c.Event)
			}
		})
	}
}

// TestEntityModelDecisionTables renders each default entity's model as a decision
// table — a server-free, readable "living doc" of how the entity behaves (the
// coverage denominator for future exploration) — and asserts the model has no dead
// events (every declared event is a live transition from some reachable state).
func TestEntityModelDecisionTables(t *testing.T) {
	entities := map[string]umpire.Lifecycled{
		"Workflow":     NewWorkflow(),
		"WorkflowTask": NewWorkflowTask(),
	}
	names := make([]string, 0, len(entities))
	for n := range entities {
		names = append(names, n)
	}
	sort.Strings(names)

	for _, name := range names {
		lc := entities[name].Lifecycle()
		cells := lc.Cells()

		t.Logf("%s model: %d reachable states, %d events, %d cells",
			name, len(lc.Reachable()), len(lc.Events()), len(cells))
		for _, c := range cells {
			if c.Kind == umpire.Advance {
				t.Logf("  %-14s --%s--> %s", c.From, c.Event, c.To)
			}
		}

		advanced := map[string]bool{}
		for _, c := range cells {
			if c.Kind == umpire.Advance {
				advanced[c.Event] = true
			}
		}
		for _, ev := range lc.Events() {
			require.Truef(t, advanced[ev], "%s: event %q is never a live transition (dead event)",
				name, ev)
		}
	}
}
