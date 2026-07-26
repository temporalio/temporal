package tests

// Model-based conformance entry points for the standalone-activity (SAA) surface. model.Transition is
// the specification; these tests drive a real onebox server through the same event alphabet and assert
// it conforms at every step. The RPC-graph and random-walk explorers are here; the wall-clock scenarios
// are covered by Test{StartDelay,Backoff,Timeout}_Declarative in
// activity_parity_with_real_drivers_test.go. The engine is in activity_standalone_conformance.go.

import (
	"math/rand"
	"testing"
	"time"

	"go.temporal.io/server/common/testing/testcontext"
)

func (s *activityParityTestSuite) TestConformance() {
	testcontext.For(s.T(), testcontext.WithTimeout(saaConformanceContextBudget()))
	s.T().Run("RPCGraphTraversal", s.conformanceRPCGraphTraversal)
	s.T().Run("RandomWalk", s.conformanceRandomWalk)
}

// saaTraversalConfigs are the activity configurations the graph traversal and random walk explore.
var saaTraversalConfigs = []activityConfig{
	{}, // no schedule-to-close, unlimited attempts
	{ScheduleToClose: activityLongDuration, ScheduleToStart: activityLongDuration, HeartbeatTimeout: activityLongDuration, MaxAttempts: 3},
	// Retries exhaust after the first attempt, putting the retryable-failure-with-no-retries-left edge at
	// depth 2 rather than past the depth bound.
	{MaxAttempts: 1},
	// No RPC event leaves StartDelayPending, so the activity stays in the start-delay window for the whole
	// traversal: this crosses every operator command with that window, and the per-Poll negative poll
	// checks that none of them dispatches early. The second adds schedule-to-close.
	{StartDelay: activityLongDuration},
	{StartDelay: activityLongDuration, ScheduleToClose: activityLongDuration},
}

// saaConformanceContextBudget is TestConformance's overall context deadline: the larger of
// DefaultTimeout (which reflects TEMPORAL_TEST_TIMEOUT) and a floor that fits both explorers at their
// default depths.
func saaConformanceContextBudget() time.Duration {
	const floor = 8 * time.Minute
	if d := testcontext.DefaultTimeout(); d > floor {
		return d
	}
	return floor
}

// conformanceRPCGraphTraversal walks the graph model.Transition describes, verifying every edge against a
// real server. From each reachable state it tries every event, replaying the path on a fresh activity,
// and asserts:
//   - the resulting internal state equals Transition().Next in every observable field;
//   - each stamp's change matches Transition()'s AttemptTasksInvalidated / ScheduleToCloseTaskInvalidated;
//   - the RPC's accept/reject outcome matches Transition().Reject;
//   - for a heartbeat, the response flags equal ExpectedHeartbeatFlags.
//
// model.Transition is total over the RPC alphabet, so a cell it does not handle panics and fails the run.
// Timeouts are configured long so none fires mid-scenario; the retry backoff is short so retries can be
// traversed.
func (s *activityParityTestSuite) conformanceRPCGraphTraversal(t *testing.T) {
	for i, cfg := range saaTraversalConfigs {
		// A namespace per config: a traversal gives every activity its own task queue, and matching's
		// per-namespace user-data propagation is rate limited over them.
		//
		// The driver anchors on the subtest t, not s.T(): the suite context is memoized once per suite
		// test, so all TestConformance subtests would otherwise share a single budget.
		d := newSAADriver(t, newActivityParityEnv(s.T()), cfg)
		d.cfgIdx = i
		d.traverse(t)
	}
}

// conformanceRandomWalk drives one activity forward through randomly chosen events: no replay, no
// backtracking, no state dedup, ~one RPC per step. It reaches the long interaction sequences the
// depth-bounded traversal structurally never visits. Every step is checked against model.Transition via
// the same apply(). The walk is deterministic in its seed, which is logged, so a failure reproduces with
// TEMPORAL_SAASPEC_WALK_SEED. Deep runs need a raised TEMPORAL_TEST_TIMEOUT and go test -timeout.
func (s *activityParityTestSuite) conformanceRandomWalk(t *testing.T) {
	seed, steps := saaWalkSeed(), saaWalkSteps()
	t.Logf("random walk: seed=%d steps=%d/cfg (override TEMPORAL_SAASPEC_WALK_SEED / _WALK_STEPS)", seed, steps)

	for i, cfg := range saaTraversalConfigs {
		d := newSAADriver(t, newActivityParityEnv(s.T()), cfg) // a namespace per config; see conformanceRPCGraphTraversal
		d.cfgIdx = i
		// Independent, reproducible RNG stream per config.
		d.randomWalk(t, rand.New(rand.NewSource(seed+int64(i))), steps)
	}
}
