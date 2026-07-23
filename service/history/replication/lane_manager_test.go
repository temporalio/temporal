package replication

import (
	"math"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
)

type laneManagerSuite struct {
	suite.Suite
	*require.Assertions
}

func TestLaneManagerSuite(t *testing.T) {
	suite.Run(t, new(laneManagerSuite))
}

func (s *laneManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

// newManager builds a lane manager and primes the default cursor.
func (s *laneManagerSuite) newManager(tierCount, demotionCycles, cooldownCycles int, defaultCursor int64) *laneManager {
	m := newLaneManager(tierCount, demotionCycles, cooldownCycles)
	m.AdvanceDefaultCursor(defaultCursor)
	return m
}

// rc reconciles with `acked` used as both the HIGH acked watermark (the split floor)
// and every tier's acked watermark (the demote floor / graduation gate). Convenient for
// tests that treat "applied" as a single position; tests that need the tier to lag the
// default cursor call Reconcile directly.
func (s *laneManagerSuite) rc(m *laneManager, acked int64, ns ...string) {
	tierAcked := make([]int64, m.TierCount())
	for i := range tierAcked {
		tierAcked[i] = acked
	}
	m.Reconcile(ns, acked, tierAcked)
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

// --- Default lane / no throttle ---

func (s *laneManagerSuite) TestNoThrottle_AllOnDefault() {
	m := s.newManager(2, 2, 1, 100)

	cursor, filter := m.DefaultBatchStart()
	s.Equal(int64(100), cursor)
	s.True(filter("a", 0))
	s.True(filter("b", 0))

	s.Equal(0, m.NamespaceTier("a"))
	s.Equal(int64(100), m.EffectiveLowCursor()) // empty tiers (MaxInt64) ignored
	s.Empty(m.AllThrottledNamespaces())
}

// --- Split ---

func (s *laneManagerSuite) TestSplit_MovesToTier1AndFloorsAtAckedWatermark() {
	m := s.newManager(2, 2, 1, 100)

	// Split floor is the HIGH acked watermark (80), which lags the default cursor (100):
	// the tier resumes from an already-applied point so no workflow straddles the boundary.
	m.AdvanceDefaultCursor(100)
	m.Reconcile([]string{"a"}, 80, []int64{80, 80})

	s.Equal(1, m.NamespaceTier("a"))
	s.Equal(int64(80), m.TierCursor(1)) // seeded at the acked watermark
	floor, ok := m.NamespaceFloor("a")
	s.True(ok)
	s.Equal(int64(80), floor)
	s.Equal([]string{"a"}, m.AllThrottledNamespaces())
	s.Equal([]string{"a"}, m.TierMembers(1))

	_, dFilter := m.DefaultBatchStart()
	s.False(dFilter("a", 150)) // excluded from default
	s.True(dFilter("b", 150))

	tCursor, tFilter := m.TierBatchStart(1)
	s.Equal(int64(80), tCursor)
	s.False(tFilter("a", 50))  // below floor: already applied, not re-sent
	s.True(tFilter("a", 80))   // at floor
	s.True(tFilter("a", 150))  // above floor
	s.False(tFilter("b", 150)) // not a member

	s.Equal(int64(80), m.EffectiveLowCursor())
}

// The floor is exactly what prevents a namespace joining a lagging tier from
// having its already-sent history re-sent.
func (s *laneManagerSuite) TestSplit_JoinerNotResentBelowFloor() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a") // tier1 cursor = 100, floor[a] = 100
	m.AdvanceDefaultCursor(200)

	// b is newly throttled with HIGH acked watermark 200; it joins the lagging tier 1
	// (still at 100) but must NOT be re-sent over [100, 200) — already applied.
	s.rc(m, 200, "a", "b")

	s.Equal(int64(100), m.TierCursor(1)) // not rewound; a keeps draining from 100
	bFloor, ok := m.NamespaceFloor("b")
	s.True(ok)
	s.Equal(int64(200), bFloor)
	s.Equal([]string{"a", "b"}, sorted(m.TierMembers(1)))

	_, tFilter := m.TierBatchStart(1)
	s.True(tFilter("a", 100))  // a still sent from its floor
	s.False(tFilter("b", 100)) // b NOT re-sent over its already-covered history
	s.False(tFilter("b", 199))
	s.True(tFilter("b", 200)) // b sent only from its floor onward
}

// --- Demotion ---

func (s *laneManagerSuite) TestDemotion_AfterConsecutiveThrottles() {
	m := s.newManager(2, 2, 1, 100)

	s.rc(m, 100, "a") // tier1, streak=1
	s.Equal(1, m.NamespaceTier("a"))
	s.rc(m, 100, "a") // streak=2 >= 2 -> demote to tier2, floored at tier1 acked (100)
	s.Equal(2, m.NamespaceTier("a"))

	s.Equal(int64(math.MaxInt64), m.TierCursor(1)) // tier1 now empty
	s.Equal(int64(100), m.TierCursor(2))           // seeded from source tier acked watermark
	floor, _ := m.NamespaceFloor("a")
	s.Equal(int64(100), floor) // floored at the source tier acked watermark
	s.Empty(m.TierMembers(1))
	s.Equal([]string{"a"}, m.TierMembers(2))
	s.Equal(int64(100), m.EffectiveLowCursor())
}

func (s *laneManagerSuite) TestDemotion_StopsAtDeepestTier() {
	m := s.newManager(2, 2, 1, 100)
	s.rc(m, 100, "a")
	s.rc(m, 100, "a") // -> tier2 (deepest)
	s.Equal(2, m.NamespaceTier("a"))

	// Keep throttling: must not demote past the deepest tier.
	s.rc(m, 100, "a")
	s.rc(m, 100, "a")
	s.rc(m, 100, "a")
	s.Equal(2, m.NamespaceTier("a"))
}

func (s *laneManagerSuite) TestDemotion_RewindsTargetAheadOfFloor() {
	// Demote into a deeper tier that is ahead of the demoted namespace's floor: the
	// target rewinds to the floor so the demoted namespace is covered with no gap
	// (re-reading a bounded window for the existing member, idempotent).
	m := s.newManager(2, 2, 1, 100)
	s.rc(m, 100, "x")
	s.rc(m, 100, "x") // x -> tier2 at 100
	s.True(m.AdvanceTierCursor(2, 100, 500))
	s.Equal(int64(500), m.TierCursor(2))

	m.AdvanceDefaultCursor(300)
	s.rc(m, 300, "a", "x") // a -> tier1 floored at HIGH acked 300; x stays tier2
	s.Equal(int64(300), m.TierCursor(1))
	s.rc(m, 300, "a", "x") // a streak=2 -> demote to tier2; floor 300 < 500 -> rewind
	s.Equal(2, m.NamespaceTier("a"))
	s.Equal(int64(300), m.TierCursor(2)) // rewound to cover a from its floor
	aFloor, _ := m.NamespaceFloor("a")
	s.Equal(int64(300), aFloor)
	xFloor, _ := m.NamespaceFloor("x")
	s.Equal(int64(100), xFloor)
	s.Equal([]string{"a", "x"}, sorted(m.TierMembers(2)))

	_, tFilter := m.TierBatchStart(2)
	s.True(tFilter("x", 300))  // x re-read in [300,500) (idempotent), still sent
	s.False(tFilter("a", 250)) // a not sent below its floor
	s.True(tFilter("a", 300))
}

// --- Merge-back (best-effort, gap-free) ---

func (s *laneManagerSuite) TestMergeBack_GatedOnAppliedWatermark() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a") // tier1 at 100
	m.AdvanceDefaultCursor(200)

	// Un-throttled, but the tier has only APPLIED up to 100 while the default lane would
	// resume from 200: graduating now would leave a gap, so it must not.
	m.Reconcile(nil, 200, []int64{100, 100})
	s.Equal(1, m.NamespaceTier("a"))

	// Once the tier has applied up to the default cursor, graduation is gap-free.
	m.Reconcile(nil, 200, []int64{200, 200})
	s.Equal(0, m.NamespaceTier("a"))
	s.Equal(int64(math.MaxInt64), m.TierCursor(1)) // tier emptied
	_, dFilter := m.DefaultBatchStart()
	s.True(dFilter("a", 250)) // back on default
}

func (s *laneManagerSuite) TestMergeBack_RespectsCooldown() {
	m := s.newManager(2, 5, 3, 100)
	s.rc(m, 100, "a") // tier1 at 100, applied up to the default cursor

	// Caught up immediately, but cooldown=3 calm acks required before graduating.
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=1
	s.Equal(1, m.NamespaceTier("a"))
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=2
	s.Equal(1, m.NamespaceTier("a"))
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=3 -> graduate
	s.Equal(0, m.NamespaceTier("a"))
}

// Best-effort: on a busy shard a rate-limited tier may never catch the racing default
// cursor, so a calm namespace stays isolated (harmless — its rate limit isn't binding)
// until the tier finally applies up to the default cursor.
func (s *laneManagerSuite) TestMergeBack_BestEffortStaysIsolatedWhileTierLags() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a")            // tier1 floored at 100
	m.AdvanceDefaultCursor(5000) // default races ahead (busy shard)

	// Calm and past cooldown, but tier applied (1000) < default (5000): stays isolated.
	for range 5 {
		m.Reconcile(nil, 5000, []int64{1000, 1000})
		s.Equal(1, m.NamespaceTier("a"))
	}

	// When the now-calm tier finally applies up to the default cursor, it graduates.
	m.Reconcile(nil, 5000, []int64{5000, 5000})
	s.Equal(0, m.NamespaceTier("a"))
	_, dFilter := m.DefaultBatchStart()
	s.True(dFilter("a", 6000))
}

// A deleted/deregistered or genuinely-calm namespace stops appearing in the throttled
// set; graduation removes it from membership (hence from the persisted predicate) once
// the tier has applied up to the default cursor.
func (s *laneManagerSuite) TestMergeBack_CalmNamespaceLeavesTierPredicate() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a")
	m.AdvanceDefaultCursor(300)
	m.Reconcile(nil, 300, []int64{300, 300}) // tier applied 300 >= default 300 -> graduate
	s.Equal(0, m.NamespaceTier("a"))
	s.Empty(m.TierMembers(1))           // gone from the tier predicate -> not persisted
	s.Empty(m.AllThrottledNamespaces()) // and from in-memory state
}

// Re-throttling resets the calm streak, so a brief calm spell does not graduate a
// namespace that is still oscillating.
func (s *laneManagerSuite) TestMergeBack_CooldownResetByRethrottle() {
	m := s.newManager(2, 5, 2, 100) // cooldown=2
	s.rc(m, 100, "a")
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=1
	s.Equal(1, m.NamespaceTier("a"))

	s.rc(m, 100, "a")                        // re-throttled -> calm streak reset
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=1 again (not 2)
	s.Equal(1, m.NamespaceTier("a"))
	m.Reconcile(nil, 100, []int64{100, 100}) // calm=2 -> graduate
	s.Equal(0, m.NamespaceTier("a"))
}

// --- Oscillation ---

func (s *laneManagerSuite) TestOscillation_StaysIsolatedWhileLagging() {
	m := s.newManager(2, 10, 1, 100) // high demotion threshold so it won't ratchet
	s.rc(m, 100, "a")                // tier1 at 100
	m.AdvanceDefaultCursor(200)      // default moves ahead; tier1 lags

	for range 5 {
		m.Reconcile(nil, 200, []int64{100, 100}) // un-throttled but lagging -> stays
		s.Equal(1, m.NamespaceTier("a"))
		s.rc(m, 200, "a") // re-throttled -> stays, no demote (D=10)
		s.Equal(1, m.NamespaceTier("a"))
	}
	// Never bounced back to the default lane while lagging.
}

// --- EffectiveLowCursor / cleanup floor ---

func (s *laneManagerSuite) TestEffectiveLowCursor_HeldByLaggingTier() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a")           // tier1 at 100
	m.AdvanceDefaultCursor(500) // default races ahead

	// Lagging tier1 (100) holds the floor even though default is at 500.
	s.Equal(int64(100), m.EffectiveLowCursor())
}

// --- Cursor CAS / rewind safety ---

func (s *laneManagerSuite) TestAdvanceTierCursor_CASRejectsStale() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a") // tier1 at 100

	s.True(m.AdvanceTierCursor(1, 100, 200))
	s.False(m.AdvanceTierCursor(1, 100, 300)) // stale "from"
	s.Equal(int64(200), m.TierCursor(1))
}

func (s *laneManagerSuite) TestAdvanceTierCursor_RewoundByReconcileFailsStaleAdvance() {
	m := s.newManager(2, 5, 1, 100)
	s.rc(m, 100, "a")                        // tier1 at 100
	s.True(m.AdvanceTierCursor(1, 100, 300)) // a's loop advances to 300

	// A new split whose floor (HIGH acked 100) is below tier1's advanced cursor (300)
	// rewinds tier1 to 100 to cover the joiner with no gap.
	s.rc(m, 100, "a", "b")
	s.Equal(int64(100), m.TierCursor(1))

	// a's in-flight loop tries to commit its old batch (from=300): rejected, so it
	// re-reads the rewound cursor and re-sends [100,300) (idempotent).
	s.False(m.AdvanceTierCursor(1, 300, 400))
	s.Equal(int64(100), m.TierCursor(1))
}

func (s *laneManagerSuite) TestReconstruct_RestoresTiersCursorsAndFloors() {
	tiers := []reconstructedTier{
		{tier: 1, cursor: 4000, members: []string{"a"}},
		{tier: 2, cursor: 1500, members: []string{"b", "c"}},
	}
	m := newLaneManagerWithState(2, 5, 1, 5000, tiers)

	s.Equal(int64(5000), m.DefaultCursor())
	s.Equal(1, m.NamespaceTier("a"))
	s.Equal(2, m.NamespaceTier("b"))
	s.Equal(2, m.NamespaceTier("c"))
	s.Equal(int64(4000), m.TierCursor(1))
	s.Equal(int64(1500), m.TierCursor(2))

	fa, ok := m.NamespaceFloor("a")
	s.True(ok)
	s.Equal(int64(4000), fa) // floor = resume cursor
	fb, _ := m.NamespaceFloor("b")
	s.Equal(int64(1500), fb)

	// Default lane excludes the restored throttled namespaces (no burst on reconnect).
	_, dFilter := m.DefaultBatchStart()
	s.False(dFilter("a", 9999))
	s.False(dFilter("b", 9999))
	s.True(dFilter("healthy", 9999))

	// Tier filters admit members only at/above their floor.
	_, t2 := m.TierBatchStart(2)
	s.False(t2("b", 1499))
	s.True(t2("b", 1500))
	s.True(t2("c", 2000))

	// Cleanup floor reflects the most-lagging restored tier.
	s.Equal(int64(1500), m.EffectiveLowCursor())

	// Reconcile works on restored state: un-throttled tiers that have not applied up to
	// the default cursor don't prematurely graduate.
	m.Reconcile([]string{"a"}, 4000, []int64{4000, 1500})
	s.Equal(1, m.NamespaceTier("a"))
	s.Equal(2, m.NamespaceTier("b")) // un-throttled but tier applied 1500 < 5000 -> stays
}

func (s *laneManagerSuite) TestReconstruct_EmptyStateIsClean() {
	m := newLaneManagerWithState(2, 5, 1, 100, nil)
	s.Equal(int64(100), m.DefaultCursor())
	s.Equal(0, m.NamespaceTier("a"))
	s.Equal(int64(100), m.EffectiveLowCursor())
}

func (s *laneManagerSuite) TestPersistenceRoundTrip() {
	// tier 2 deliberately left empty to exercise positional encoding + empty skipping.
	lm := newLaneManagerWithState(3, 5, 1, 6000, []reconstructedTier{
		{tier: 1, cursor: 4000, members: []string{"a"}},
		{tier: 3, cursor: 1000, members: []string{"b", "c"}},
	})
	sender := &StreamSenderImpl{laneManager: lm}
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1000,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 6000},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 5000},
		ThrottledLaneStates: []*replicationspb.ReplicationState{
			{InclusiveLowWatermark: 4000}, // tier 1
			{InclusiveLowWatermark: 0},    // tier 2 (empty; watermark unused)
			{InclusiveLowWatermark: 1000}, // tier 3
		},
	}

	readerState := sender.buildTieredIsolationReaderState(attr)
	s.Len(readerState.Scopes, 6)                                          // 3 base + 3 tiers (positional)
	s.Equal(int64(1000), readerState.Scopes[0].Range.InclusiveMin.TaskId) // overall min -> cleanup floor
	s.Equal(int64(6000), readerState.Scopes[1].Range.InclusiveMin.TaskId) // default-HIGH
	// default-HIGH excludes all throttled namespaces.
	s.Equal(enumsspb.PREDICATE_TYPE_NOT, readerState.Scopes[1].Predicate.PredicateType)
	s.Equal(int64(5000), readerState.Scopes[2].Range.InclusiveMin.TaskId) // LOW (single lane)
	s.Equal(enumsspb.PREDICATE_TYPE_UNIVERSAL, readerState.Scopes[2].Predicate.PredicateType)
	// tier 1 scope carries its members.
	s.Equal(enumsspb.PREDICATE_TYPE_NAMESPACE_ID, readerState.Scopes[3].Predicate.PredicateType)
	s.Equal([]string{"a"}, readerState.Scopes[3].Predicate.GetNamespaceIdPredicateAttributes().NamespaceIds)

	defaultCursor, tiers := parseLaneState(readerState, 3)
	s.Equal(int64(6000), defaultCursor) // default-HIGH resume cursor
	s.Len(tiers, 2)                     // empty tier 2 dropped

	byTier := map[int]reconstructedTier{}
	for _, t := range tiers {
		byTier[t.tier] = t
	}
	s.Equal(int64(4000), byTier[1].cursor)
	s.Equal([]string{"a"}, byTier[1].members)
	s.Equal(int64(1000), byTier[3].cursor)
	s.Equal([]string{"b", "c"}, sorted(byTier[3].members))

	// Reconstructing from the parsed state reproduces the membership and cursors.
	restored := newLaneManagerWithState(3, 5, 1, defaultCursor, tiers)
	s.Equal(1, restored.NamespaceTier("a"))
	s.Equal(3, restored.NamespaceTier("b"))
	s.Equal(int64(4000), restored.TierCursor(1))
	s.Equal(int64(1000), restored.TierCursor(3))
}

func (s *laneManagerSuite) TestReconcile_RepeatedThrottleIsIdempotentMembership() {
	m := s.newManager(3, 100, 1, 100) // very high demotion threshold
	s.rc(m, 100, "a")
	s.rc(m, 100, "a")
	s.rc(m, 100, "a")
	s.Equal(1, m.NamespaceTier("a"))
	s.Equal([]string{"a"}, m.TierMembers(1))
}
