package tests

// Model-based spec entry points for the standalone-activity (SAA) product surface. model.Transition
// is the spec — a total function Transition(cfg, state, event) -> Outcome — and these tests drive a
// real onebox server through the same event alphabet, asserting the server agrees with the model at
// every step. The wall-clock scenarios (timeouts, dispatch delays) are checked by the declarative
// trace tests (Test{StartDelay,Backoff,Timeout}_Declarative in activity_standalone_test.go), which
// model-check each step via driveTrace; the exhaustive RPC-graph and random-walk explorers are here.
// The engine that drives and checks each event is in activity_standalone_spec_harness.go, built on
// the driver in activity_standalone_utils.go.

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm/lib/activity/model"
	"go.temporal.io/server/common/testing/testcontext"
)

func (s *standaloneActivityTestSuite) TestSpec() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaSpecContextBudget()))
	s.T().Run("RPCGraphTraversal", s.specRPCGraphTraversal)
	s.T().Run("RandomWalk", s.specRandomWalk)
}

// saaTraversalConfigs are the activity configurations the graph traversal and random walk explore.
var saaTraversalConfigs = []model.Config{
	{}, // no schedule-to-close, unlimited attempts
	{HasScheduleToClose: true, HasScheduleToStart: true, HasHeartbeat: true, MaxAttempts: 3},
	// Retries exhaust after the first attempt, so the RespondFailed exhaustion boundary
	// (retryable failure with no retries left -> Failed) is reached at depth 2 rather than
	// past the depth bound. See the completeness check.
	{MaxAttempts: 1},
	// Start-delay window: the activity stays StartDelayPending for the whole traversal (no RPC event
	// leaves that state), so this crosses the operator commands (pause/unpause/reset/update/cancel/
	// terminate) with the start-delay window and verifies via the per-Poll negative poll that none of
	// them dispatches early. The second adds schedule-to-close so its window-invalidation is exercised.
	{HasStartDelay: true},
	{HasStartDelay: true, HasScheduleToClose: true},
}

// saaSpecContextBudget is TestSpec's overall context deadline. DefaultTimeout already reflects
// TEMPORAL_TEST_TIMEOUT, so take the larger of it and a floor generous enough for the combined
// explorers at their default depths.
func saaSpecContextBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// specRPCGraphTraversal walks the transition graph that model.Transition describes and verifies every
// edge against a real onebox server. From each reachable state it tries every event, replays the path
// from a fresh activity, drives the event as a real RPC, reads the internal state back with
// ReadComponent, and asserts:
//   - the resulting internal state equals Transition().Next exactly (all observable fields);
//   - each stamp's change across the edge matches Transition()'s AttemptTasksInvalidated / ScheduleToCloseTaskInvalidated;
//   - the RPC's accept/reject outcome matches Transition().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
//
// model.Transition is total over the RPC event alphabet; a cell it does not handle panics and fails
// the run. Timeouts are configured long (hours) so none fires mid-scenario; retry backoff is short so
// retries can be traversed. Timeout timing is checked by the traces, not here.
func (s *standaloneActivityTestSuite) specRPCGraphTraversal(t *testing.T) {
	env := s.newTestEnv()
	// testcontext.For(t), not s.Context(): the suite context is memoized once per suite test, so all
	// TestSpec subtests would otherwise share a single budget. Anchoring on the subtest t gives each
	// explorer its own budget.
	ctx := testcontext.For(t)

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env:      env,
			ctx:      ctx,
			chasmCtx: chasmCtx,
			nsID:     env.NamespaceID().String(),
			cfg:      cfg,
			cfgIdx:   i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole traversal so the activity stays
			// StartDelayPending (the model never leaves that state via an RPC event), letting the BFS
			// cross the operator commands with the start-delay window.
			h.startDelay = time.Hour
		}
		h.traverse(t)
	}
}

// specRandomWalk drives one activity forward through randomly chosen events — no replay, no
// backtracking, no state dedup. Where the graph traversal is exhaustive but depth-bounded, this
// reaches deep, long interaction sequences the bounded BFS structurally never visits, at ~one RPC per
// step. Every step is checked against model.Transition (via the same apply()), so a divergence is
// caught the same way; the walk is deterministic in its seed (logged), so any failure reproduces
// exactly with TEMPORAL_SAASPEC_WALK_SEED. Deep runs need a raised budget: TEMPORAL_TEST_TIMEOUT and
// the go test -timeout. Set TEMPORAL_SAASPEC_NO_NEGATIVE_POLL=1 to skip the ~3s Paused negative poll.
func (s *standaloneActivityTestSuite) specRandomWalk(t *testing.T) {
	env := s.newTestEnv()
	ctx := testcontext.For(t) // subtest-scoped budget; see specRPCGraphTraversal

	chasmCtx, err := env.GetTestCluster().Host().ChasmContext(ctx)
	require.NoError(t, err)

	seed, steps := saaWalkSeed(), saaWalkSteps()
	t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range saaTraversalConfigs {
		h := &saaHarness{
			env: env, ctx: ctx, chasmCtx: chasmCtx, nsID: env.NamespaceID().String(),
			cfg: cfg, cfgIdx: i,
		}
		if cfg.HasStartDelay {
			// Keep the first-dispatch window open for the whole walk so the activity stays
			// StartDelayPending (no RPC event leaves it); the walk explores operator commands in the
			// window and, unlike the BFS, re-polls post-operation states (catching early re-dispatch).
			h.startDelay = time.Hour
		}
		// Independent, reproducible RNG stream per config.
		h.randomWalk(t, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}
