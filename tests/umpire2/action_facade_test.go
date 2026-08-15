package umpire2

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/testing/umpire"
)

func TestActionRunResultReturnsDefensivePlanAndDrift(t *testing.T) {
	result := &ActionRunResult{
		plan:  []umpire.Action{{Name: "advance"}},
		drift: []umpire.Drift{{Action: "advance", Reason: "not observed"}},
	}
	plan := result.Plan()
	drift := result.Drift()
	plan[0].Name = "changed"
	drift[0].Reason = "changed"

	require.Equal(t, "advance", result.Plan()[0].Name)
	require.Equal(t, "not observed", result.Drift()[0].Reason)
}
