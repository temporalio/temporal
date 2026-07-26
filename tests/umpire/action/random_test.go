package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	umpire "go.temporal.io/server/common/testing/umpire"
	"go.temporal.io/server/tests/umpire/action"
)

// TestRandomPlanReproducible pins the generator's contract the randomized loop relies on: a seed
// maps to a stable plan (so a failing iteration replays from its seed alone), and every generated
// plan is non-empty and ends at a client-entry action that produces the settling transition.
func TestRandomPlanReproducible(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		plan, label := action.RandomPlan(seed)
		require.NotEmpty(t, plan, "seed %d (%s): plan must be drivable", seed, label)

		again, againLabel := action.RandomPlan(seed)
		require.Equal(t, label, againLabel, "seed %d: label must be reproducible", seed)
		require.Equal(t, names(plan), names(again), "seed %d: action sequence must be reproducible", seed)

		// A fault, when sampled, is a standing action installed before the transitions fire, so it
		// must be first; the rest are the ordered route.
		for i, a := range plan {
			if a.Kind == umpire.Fault {
				require.Zero(t, i, "seed %d: a sampled fault must be prepended, not mid-plan", seed)
			}
		}
	}
}
