package tests

// Model-based conformance entry point for workflow activities (WFA). The same model.Transition that
// specifies standalone activity specifies this surface too, so the divergences the random walk finds are
// the equivalence failures a future WFA-on-CHASM port would inherit.
//
// The walk only, not the RPC-graph traversal: that replays each path on a fresh activity, which here
// means a workflow start per edge. The adapter is in activity_workflow_conformance.go.

import (
	"math/rand"
	"testing"

	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
)

// wfaTraversalConfigs are the activity configurations the WFA random walk explores: saaTraversalConfigs
// without the two start-delay ones, because a workflow activity has no per-activity start delay.
var wfaTraversalConfigs = []activityConfig{
	{}, // no schedule-to-close, unlimited attempts
	{ScheduleToClose: activityLongDuration, ScheduleToStart: activityLongDuration, HeartbeatTimeout: activityLongDuration, MaxAttempts: 3},
	// Retries exhaust after the first attempt, putting the retryable-failure-with-no-retries-left edge at
	// depth 2 rather than past the depth bound.
	{MaxAttempts: 1},
}

func (s *activityParityTestSuite) conformanceWFARandomWalk(t *testing.T) {
	testcontext.For(t, testcontext.WithTimeout(saaConformanceContextBudget()))
	seed, steps := model.WalkSeed(), model.WalkSteps()
	t.Logf("WFA random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range wfaTraversalConfigs {
		d := newWFADriver(t, newActivityParityEnv(s.T()), cfg) // a namespace per config; see conformanceRPCGraphTraversal
		d.cfgIdx = i
		// Keep each wrapper workflow running once its activity closes, so an RPC driven from a terminal
		// state is answered about the activity rather than about a workflow that no longer exists.
		d.holdOpen = true
		// Independent, reproducible RNG stream per config.
		activityRandomWalk(t, d, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}
