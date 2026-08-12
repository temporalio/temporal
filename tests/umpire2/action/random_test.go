package action_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tests/umpire2/action"
)

// TestRandomPlanReproducible pins the generator's contract the randomized loop relies on: a seed
// maps to a stable, non-empty plan, so a failing iteration replays from its seed alone.
func TestRandomPlanReproducible(t *testing.T) {
	for seed := int64(0); seed < 32; seed++ {
		plan, label := action.RandomPlan(seed)
		require.NotEmpty(t, plan, "seed %d (%s): plan must be drivable", seed, label)

		again, againLabel := action.RandomPlan(seed)
		require.Equal(t, label, againLabel, "seed %d: label must be reproducible", seed)
		require.Equal(t, names(plan), names(again), "seed %d: action sequence must be reproducible", seed)
	}
}
