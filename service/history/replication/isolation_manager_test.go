package replication

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

type isolationManagerSuite struct {
	suite.Suite
	*require.Assertions
}

func TestIsolationManagerSuite(t *testing.T) {
	suite.Run(t, new(isolationManagerSuite))
}

func (s *isolationManagerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

// rc reconciles with `acked` used as both the shared HIGH acked watermark (the split
// floor) and every member lane's acked watermark (the merge-back gate). Convenient
// for tests that treat "applied" as a single position.
func (s *isolationManagerSuite) rc(m *isolationManager, acked int64, ns ...string) {
	memberAcked := make(map[string]int64, len(ns))
	for _, n := range ns {
		memberAcked[n] = acked
	}
	m.Reconcile(ns, acked, memberAcked)
}

// advance moves a member's cursor under its current generation.
func (s *isolationManagerSuite) advance(m *isolationManager, ns string, to int64) {
	snap, ok := s.member(m, ns)
	s.True(ok, "advance: %s is not a member", ns)
	m.AdvanceMemberCursor(ns, snap.generation, to)
}

// nsTask builds a replication queue task for filter assertions.
func nsTask(namespaceID string, taskID int64) tasks.Task {
	return &tasks.HistoryReplicationTask{
		WorkflowKey: definition.NewWorkflowKey(namespaceID, "wf", "run"),
		TaskID:      taskID,
	}
}

// admits reports whether a lane filter admits the given namespace's task; a nil
// filter admits everything.
func admits(filter func(tasks.Task) bool, namespaceID string, taskID int64) bool {
	if filter == nil {
		return true
	}
	return filter(nsTask(namespaceID, taskID))
}

func sorted(in []string) []string {
	out := append([]string(nil), in...)
	slices.Sort(out)
	return out
}

func (s *isolationManagerSuite) member(m *isolationManager, ns string) (memberSnapshot, bool) {
	for tier := 1; tier <= m.TierCount(); tier++ {
		for _, snap := range m.TierMemberSnapshots(tier) {
			if snap.namespaceID == ns {
				return snap, true
			}
		}
	}
	return memberSnapshot{}, false
}

func (s *isolationManagerSuite) TestNoThrottle_AllOnShared() {
	m := newIsolationManager(2, 2, 1, 0)
	s.Nil(m.DefaultFilter())
	s.Equal(0, m.NamespaceTier("a"))
	s.Empty(m.TierMembers(1))
}

func (s *isolationManagerSuite) TestSplit_OwnLaneFlooredAtAckedWatermark() {
	m := newIsolationManager(2, 2, 1, 0)
	m.AdvanceDefaultCursor(100)

	// Split floor is the shared HIGH acked watermark (80), which lags the shared
	// cursor (100): the lane starts from an already-applied point so no workflow
	// straddles the hand-off; the shared in-flight [80, 100) window double-applies
	// idempotently.
	m.Reconcile([]string{"a"}, 80, nil)

	s.Equal(1, m.NamespaceTier("a"))
	snap, ok := s.member(m, "a")
	s.True(ok)
	s.Equal(int64(80), snap.cursor)
	s.False(admits(snap.scope.Contains, "a", 79)) // below floor: already applied
	s.True(admits(snap.scope.Contains, "a", 80))
	s.False(admits(snap.scope.Contains, "b", 150)) // other namespaces not owned

	dFilter := m.DefaultFilter()
	s.False(admits(dFilter, "a", 150)) // excluded from the shared lane
	s.True(admits(dFilter, "b", 150))
}

// Members never interact: a new split moves no existing member's cursor.
func (s *isolationManagerSuite) TestSplit_MembersAreIndependent() {
	m := newIsolationManager(2, 5, 1, 0)
	s.rc(m, 100, "a")
	s.advance(m, "a", 500)
	m.AdvanceDefaultCursor(300)

	s.rc(m, 300, "a", "b") // b splits at 300; a is untouched

	snapA, _ := s.member(m, "a")
	s.Equal(int64(500), snapA.cursor)
	snapB, _ := s.member(m, "b")
	s.Equal(int64(300), snapB.cursor)
	s.False(admits(snapB.scope.Contains, "b", 299))
	s.True(admits(snapB.scope.Contains, "b", 300))
}

func (s *isolationManagerSuite) TestSplit_CapLimitsIsolatedNamespaces() {
	m := newIsolationManager(2, 5, 1, 2)
	m.AdvanceDefaultCursor(100)

	// Splits follow the receiver-reported order, so c loses the race for the last slot.
	s.rc(m, 100, "a", "b", "c")
	s.Equal(1, m.NamespaceTier("a"))
	s.Equal(1, m.NamespaceTier("b"))
	s.Equal(0, m.NamespaceTier("c")) // over the cap: stays on the shared lane
	s.True(admits(m.DefaultFilter(), "c", 200))

	// a graduates (calm, lane applied up to the shared cursor), freeing a slot;
	// the remaining offender is then isolated.
	m.Reconcile([]string{"b", "c"}, 100, map[string]int64{"a": 100})
	s.Equal(0, m.NamespaceTier("a"))
	s.Equal(1, m.NamespaceTier("c"))
	s.Equal([]string{"a"}, m.PopRetired())
}

func (s *isolationManagerSuite) TestDemotion_IsARateClassChange() {
	m := newIsolationManager(2, 2, 1, 0)
	s.rc(m, 100, "a") // tier 1, streak=1
	s.advance(m, "a", 400)

	s.rc(m, 100, "a") // streak=2 -> tier 2
	s.Equal(2, m.NamespaceTier("a"))
	snap, _ := s.member(m, "a")
	s.Equal(int64(400), snap.cursor) // demotion moves no data: cursor untouched
	s.False(admits(snap.scope.Contains, "a", 99))

	// Keep throttling: never demotes past the deepest tier.
	s.rc(m, 100, "a")
	s.rc(m, 100, "a")
	s.Equal(2, m.NamespaceTier("a"))
}

func (s *isolationManagerSuite) TestMergeBack_GatedOnAppliedWatermark() {
	m := newIsolationManager(2, 5, 1, 0)
	s.rc(m, 100, "a")
	m.AdvanceDefaultCursor(200)

	// Calm, but the lane has only applied up to 100 while the shared lane resumes
	// from 200: graduating now would leave a gap, so it must not.
	m.Reconcile(nil, 200, map[string]int64{"a": 100})
	s.Equal(1, m.NamespaceTier("a"))
	s.Empty(m.PopRetired())

	// Once the lane has applied up to the shared cursor, graduation is gap-free and
	// queues a lane-retirement marker.
	m.Reconcile(nil, 200, map[string]int64{"a": 200})
	s.Equal(0, m.NamespaceTier("a"))
	s.Equal([]string{"a"}, m.PopRetired())
	s.Empty(m.PopRetired()) // popped once
	s.Nil(m.DefaultFilter())
}

func (s *isolationManagerSuite) TestMergeBack_RespectsCooldownAndRethrottleReset() {
	m := newIsolationManager(2, 5, 2, 0)
	m.AdvanceDefaultCursor(100)
	s.rc(m, 100, "a")

	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=1
	s.Equal(1, m.NamespaceTier("a"))
	s.rc(m, 100, "a")                                 // re-throttled -> calm streak reset
	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=1 again
	s.Equal(1, m.NamespaceTier("a"))
	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=2 -> graduate
	s.Equal(0, m.NamespaceTier("a"))
}

// The merge-back gate is meaningless until the shared lane's position is known: with
// defaultCursor unset (0), `acked >= defaultCursor` would be vacuously true and a
// freshly-built manager could graduate a member before any shared send was recorded,
// stranding the lane's window. Never graduate against an unset cursor.
func (s *isolationManagerSuite) TestMergeBack_NeverGraduatesAgainstUnsetSharedCursor() {
	m := newIsolationManager(2, 5, 1, 0)
	s.rc(m, 80, "a")

	m.Reconcile(nil, 80, nil) // calm, but defaultCursor is still 0
	s.Equal(1, m.NamespaceTier("a"))
	s.Empty(m.PopRetired())

	m.AdvanceDefaultCursor(90)
	m.Reconcile(nil, 90, map[string]int64{"a": 90})
	s.Equal(0, m.NamespaceTier("a"))
	s.Equal([]string{"a"}, m.PopRetired())
}

// A namespace re-throttled after graduating but before its retirement marker was
// popped must not have that marker emitted against its NEW lane.
func (s *isolationManagerSuite) TestResplit_CancelsPendingRetirement() {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(100)
	s.rc(m, 100, "a")

	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // graduates; marker queued
	s.Equal(0, m.NamespaceTier("a"))

	s.rc(m, 150, "a") // re-throttled before PopRetired ran
	s.True(m.IsMember("a"))
	s.Empty(m.PopRetired(), "pending retirement for a re-split namespace must be cancelled")
}

// A tier loop that finishes a batch from before a graduation must not fast-forward
// the namespace's NEW lane (post re-split) past tasks that lane never sent.
func (s *isolationManagerSuite) TestAdvanceMemberCursor_StaleGenerationIgnored() {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(200)
	s.rc(m, 200, "a")
	oldSnap, _ := s.member(m, "a")

	m.Reconcile(nil, 200, map[string]int64{"a": 200}) // graduate
	m.PopRetired()
	s.rc(m, 210, "a") // re-split: new incarnation at floor 210
	newSnap, _ := s.member(m, "a")
	s.NotEqual(oldSnap.generation, newSnap.generation)

	// The old incarnation's in-flight batch completes and reports progress.
	m.AdvanceMemberCursor("a", oldSnap.generation, 700)
	current, _ := s.member(m, "a")
	s.Equal(int64(210), current.cursor, "stale-generation advance must be a no-op")

	m.AdvanceMemberCursor("a", newSnap.generation, 300)
	current, _ = s.member(m, "a")
	s.Equal(int64(300), current.cursor)
}

// A stale or regressed receiver report must not rewind a lane's applied watermark.
func (s *isolationManagerSuite) TestReconcile_AckedIsMonotonic() {
	m := newIsolationManager(2, 5, 3, 0)
	m.AdvanceDefaultCursor(400)
	s.rc(m, 100, "a")

	m.Reconcile([]string{"a"}, 100, map[string]int64{"a": 300})
	m.Reconcile([]string{"a"}, 100, map[string]int64{"a": 250}) // regressed report
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1000,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 1000},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 1000},
	}
	state := m.BuildReaderState(attr)
	s.Len(state.Scopes, 4)
	s.Equal(int64(300), state.Scopes[3].GetRange().GetInclusiveMin().GetTaskId())
}

// Replication task cleanup deletes strictly below Scopes[0] and reads no other
// scope, so Scopes[0] must never run ahead of any member lane's resume position.
func (s *isolationManagerSuite) TestBuildReaderState_ClampsOverallMinToMemberFloors() {
	m := newIsolationManagerWithState(2, 5, 1, 0, 5000, []restoredMember{
		{namespaceID: "a", cursor: 800},
	})
	// Post-reconnect shape: the receiver has no member lanes yet, so its fold (and
	// both priority watermarks) can be far above the restored member's window.
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 5000,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 5000},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 5000},
	}
	state := m.BuildReaderState(attr)
	s.Equal(int64(800), state.Scopes[0].GetRange().GetInclusiveMin().GetTaskId(),
		"scope 0 must be clamped to the member's unsent window so cleanup cannot delete it")
	s.Equal(int64(5000), state.Scopes[1].GetRange().GetInclusiveMin().GetTaskId())
	floor, ok := m.MemberResumeFloor()
	s.True(ok)
	s.Equal(int64(800), floor)

	// Once the lane catches up, the clamp lifts with it.
	m.Reconcile([]string{"a"}, 5000, map[string]int64{"a": 5000})
	state = m.BuildReaderState(attr)
	s.Equal(int64(5000), state.Scopes[0].GetRange().GetInclusiveMin().GetTaskId())
	floor, ok = m.MemberResumeFloor()
	s.True(ok)
	s.Equal(int64(5000), floor)
}

func (s *isolationManagerSuite) TestMemberResumeFloor_ZeroMinimum() {
	m := newIsolationManagerWithState(2, 5, 1, 0, 5000, []restoredMember{
		{namespaceID: "zero", cursor: 0},
		{namespaceID: "positive", cursor: 800},
	})
	floor, ok := m.MemberResumeFloor()
	s.True(ok)
	s.Zero(floor)
}

func (s *isolationManagerSuite) TestCollapseToDefaultLane_GatedOnSharedAck() {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(200)
	s.rc(m, 100, "a", "b")

	// A receiver's pre-existing shared watermark is not evidence that this sender
	// re-covered the restored member windows.
	s.False(m.CollapseToDefaultLane(200))
	s.False(m.RecordDefaultLaneCoverage(101, 200))
	s.True(m.RecordDefaultLaneCoverage(100, 200))
	s.False(m.CollapseToDefaultLane(199))
	s.Equal(1, m.NamespaceTier("a"))
	s.Equal(1, m.NamespaceTier("b"))

	// New unfiltered shared-lane sends do not move the fallback gate: tasks above
	// the snapshotted cursor are already owned by the shared lane.
	m.AdvanceDefaultCursor(300)
	s.True(m.CollapseToDefaultLane(200))
	s.Nil(m.DefaultFilter())
	s.Empty(m.TierMembers(1))
	s.False(m.CollapseToDefaultLane(200))
}

func (s *isolationManagerSuite) TestCollapseToDefaultLane_DropsPendingRetirements() {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(200)
	s.rc(m, 100, "a")
	m.Reconcile(nil, 200, map[string]int64{"a": 200})

	s.True(m.CollapseToDefaultLane(200))
	s.Empty(m.PopRetired())
}

func (s *isolationManagerSuite) TestCollapseToDefaultLane_EmptyReplayAtMemberFloor() {
	m := newIsolationManager(2, 5, 1, 0)
	s.rc(m, 100, "a")

	// An exclusive queue high watermark at the inclusive member floor means there
	// are no readable tasks in the member's [floor, high) window.
	s.True(m.RecordDefaultLaneCoverage(100, 100))
	s.True(m.CollapseToDefaultLane(100))
}

// Misconfiguration must clamp, not strand: tierCount < 1 would exclude isolated
// namespaces from the shared lane while running zero tier send loops.
func (s *isolationManagerSuite) TestConfigClamps() {
	m := newIsolationManager(0, 0, 0, 0)
	s.Equal(1, m.TierCount())
	m.AdvanceDefaultCursor(100)
	s.rc(m, 100, "a")
	s.Equal(1, m.NamespaceTier("a"))
}

func (s *isolationManagerSuite) TestAdvanceMemberCursor_MonotonicAndScoped() {
	m := newIsolationManager(2, 5, 1, 0)
	s.rc(m, 100, "a")

	s.advance(m, "a", 300)
	s.advance(m, "a", 200) // never rewinds
	snap, _ := s.member(m, "a")
	s.Equal(int64(300), snap.cursor)

	m.AdvanceMemberCursor("ghost", 1, 500) // unknown member: no-op
	s.Equal(0, m.NamespaceTier("ghost"))
}

func (s *isolationManagerSuite) TestPersistenceRoundTrip() {
	m := newIsolationManagerWithState(3, 5, 1, 0, 6000, []restoredMember{
		{namespaceID: "a", cursor: 4000},
		{namespaceID: "b", cursor: 1000},
	})
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1000,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 6000},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 5000},
	}
	// b's lane has applied further since restore; its persisted resume point follows.
	m.Reconcile(nil, 6000, map[string]int64{"b": 2000})
	s.Len(m.TierMembers(1), 2)
	s.Equal(2, m.TierMemberCount(1))
	s.Equal(0, m.TierMemberCount(2))

	readerState := m.BuildReaderState(attr)
	s.Len(readerState.Scopes, 5) // 3 shared + one per member
	s.Equal(int64(1000), readerState.Scopes[0].Range.InclusiveMin.TaskId)
	s.Equal(enumsspb.PREDICATE_TYPE_NOT, readerState.Scopes[1].Predicate.PredicateType)
	s.Equal(int64(6000), readerState.Scopes[1].Range.InclusiveMin.TaskId)
	s.Equal(enumsspb.PREDICATE_TYPE_UNIVERSAL, readerState.Scopes[2].Predicate.PredicateType)

	defaultCursor, restored, err := parseIsolationState(readerState)
	s.NoError(err)
	s.Equal(int64(6000), defaultCursor)
	s.Len(restored, 2)
	byNS := map[string]int64{}
	names := make([]string, 0, len(restored))
	for _, r := range restored {
		byNS[r.namespaceID] = r.cursor
		names = append(names, r.namespaceID)
	}
	s.Equal([]string{"a", "b"}, sorted(names))
	s.Equal(int64(4000), byNS["a"]) // no ack yet: resumes at floor
	s.Equal(int64(2000), byNS["b"]) // resumes at the lane's applied watermark

	// Reconstructing reproduces membership, floors, and cursors; severity resets.
	rebuilt := newIsolationManagerWithState(3, 5, 1, 0, defaultCursor, restored)
	s.Equal(1, rebuilt.NamespaceTier("a"))
	s.Equal(1, rebuilt.NamespaceTier("b"))
	snapB, _ := s.member(rebuilt, "b")
	s.Equal(int64(2000), snapB.cursor)
	s.False(admits(rebuilt.DefaultFilter(), "a", 9999))
	s.True(admits(rebuilt.DefaultFilter(), "healthy", 9999))
}

// A lane watermark reported in the same ack that splits the namespace anchors the
// new member immediately (e.g. a restored lane the receiver is still tracking),
// rather than waiting an extra ack cycle.
func (s *isolationManagerSuite) TestSplit_ConsumesReportedWatermark() {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(100)

	m.Reconcile([]string{"a"}, 80, map[string]int64{"a": 95})
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 80,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 80},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 80},
	}
	state := m.BuildReaderState(attr)
	s.Equal(int64(95), state.Scopes[3].GetRange().GetInclusiveMin().GetTaskId())
}

// The recv loop reconciles while tier send loops snapshot members and advance
// cursors; drive both concurrently so the race detector verifies the locking.
func (s *isolationManagerSuite) TestConcurrentReconcileAndAdvance() {
	m := newIsolationManager(3, 2, 2, 0)
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 1},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 1},
	}
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Go(func() {
			ns := fmt.Sprintf("ns-%d", g)
			for i := int64(1); i <= 50; i++ {
				m.Reconcile([]string{ns}, i*10, map[string]int64{ns: i * 10})
				for _, snap := range m.TierMemberSnapshots(m.NamespaceTier(ns)) {
					m.AdvanceMemberCursor(snap.namespaceID, snap.generation, snap.cursor+10)
				}
				m.AdvanceDefaultCursor(i * 10)
				_ = m.DefaultFilter()
				_ = m.PopRetired()
				_ = m.BuildReaderState(attr)
			}
		})
	}
	wg.Wait()
}

func (s *isolationManagerSuite) TestParseIsolationState_LegacyScopesIgnored() {
	// A 3-scope state (no isolation) restores no members.
	m := newIsolationManager(2, 5, 1, 0)
	readerState := m.BuildReaderState(&replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 100,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 150},
	})
	s.Len(readerState.Scopes, 3)
	defaultCursor, restored, err := parseIsolationState(readerState)
	s.NoError(err)
	s.Equal(int64(200), defaultCursor)
	s.Empty(restored)
}

// A scope 3+ that is not a single-namespace member lane was not written by this
// codec; reject it rather than silently dropping a member (which would lift the
// scope-0 cleanup clamp over its unsent window).
func (s *isolationManagerSuite) TestParseIsolationState_MalformedScopeError() {
	m := newIsolationManager(2, 5, 1, 0)
	readerState := m.BuildReaderState(&replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 100,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 150},
	})
	universalScope := queues.ToPersistenceScope(queues.NewScope(
		queues.NewRange(tasks.NewImmediateKey(50), tasks.NewImmediateKey(math.MaxInt64)),
		predicates.Universal[tasks.Task](),
	))
	readerState.Scopes = append(readerState.Scopes, universalScope)

	_, _, err := parseIsolationState(readerState)
	s.ErrorContains(err, "unexpected predicate")
}
