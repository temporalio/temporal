package replication

import (
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

// reconcileAll reconciles with `acked` used as both the shared HIGH acked watermark
// (the split floor) and every member lane's acked watermark (the merge-back gate).
// Convenient for tests that treat "applied" as a single position.
func reconcileAll(m *isolationManager, acked int64, ns ...string) {
	memberAcked := make(map[string]int64, len(ns))
	for _, n := range ns {
		memberAcked[n] = acked
	}
	m.Reconcile(ns, acked, memberAcked)
}

// advanceMember moves a member's cursor under its current generation.
func advanceMember(t *testing.T, m *isolationManager, ns string, to int64) {
	t.Helper()
	snap, ok := findMember(m, ns)
	require.True(t, ok, "advanceMember: %s is not a member", ns)
	m.AdvanceMemberCursor(ns, snap.generation, to)
}

func findMember(m *isolationManager, ns string) (memberSnapshot, bool) {
	for tier := 1; tier <= m.TierCount(); tier++ {
		for _, snap := range m.TierMemberSnapshots(tier) {
			if snap.namespaceID == ns {
				return snap, true
			}
		}
	}
	return memberSnapshot{}, false
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

func TestIsolationManager_NoThrottle_AllOnShared(t *testing.T) {
	m := newIsolationManager(2, 2, 1, 0)
	require.Nil(t, m.DefaultFilter())
	require.Equal(t, 0, m.NamespaceTier("a"))
	require.Empty(t, m.TierMembers(1))
}

func TestIsolationManager_Split_OwnLaneFlooredAtAckedWatermark(t *testing.T) {
	m := newIsolationManager(2, 2, 1, 0)
	m.AdvanceDefaultCursor(100)

	// Split floor is the shared HIGH acked watermark (80), which lags the shared
	// cursor (100): the lane starts from an already-applied point so no workflow
	// straddles the hand-off; the shared in-flight [80, 100) window double-applies
	// idempotently.
	m.Reconcile([]string{"a"}, 80, nil)

	require.Equal(t, 1, m.NamespaceTier("a"))
	snap, ok := findMember(m, "a")
	require.True(t, ok)
	require.Equal(t, int64(80), snap.cursor)
	require.False(t, admits(snap.scope.Contains, "a", 79)) // below floor: already applied
	require.True(t, admits(snap.scope.Contains, "a", 80))
	require.False(t, admits(snap.scope.Contains, "b", 150)) // other namespaces not owned

	dFilter := m.DefaultFilter()
	require.False(t, admits(dFilter, "a", 150)) // excluded from the shared lane
	require.True(t, admits(dFilter, "b", 150))
}

// Members never interact: a new split moves no existing member's cursor.
func TestIsolationManager_Split_MembersAreIndependent(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	reconcileAll(m, 100, "a")
	advanceMember(t, m, "a", 500)
	m.AdvanceDefaultCursor(300)

	reconcileAll(m, 300, "a", "b") // b splits at 300; a is untouched

	snapA, _ := findMember(m, "a")
	require.Equal(t, int64(500), snapA.cursor)
	snapB, _ := findMember(m, "b")
	require.Equal(t, int64(300), snapB.cursor)
	require.False(t, admits(snapB.scope.Contains, "b", 299))
	require.True(t, admits(snapB.scope.Contains, "b", 300))
}

func TestIsolationManager_Split_CapLimitsIsolatedNamespaces(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 2)
	m.AdvanceDefaultCursor(100)

	// Splits follow the receiver-reported order, so c loses the race for the last slot.
	reconcileAll(m, 100, "a", "b", "c")
	require.Equal(t, 1, m.NamespaceTier("a"))
	require.Equal(t, 1, m.NamespaceTier("b"))
	require.Equal(t, 0, m.NamespaceTier("c")) // over the cap: stays on the shared lane
	require.True(t, admits(m.DefaultFilter(), "c", 200))

	// a graduates (calm, lane applied up to the shared cursor), freeing a slot;
	// the remaining offender is then isolated.
	m.Reconcile([]string{"b", "c"}, 100, map[string]int64{"a": 100})
	require.Equal(t, 0, m.NamespaceTier("a"))
	require.Equal(t, 1, m.NamespaceTier("c"))
	require.Equal(t, []string{"a"}, m.PopRetired())
}

func TestIsolationManager_Demotion_IsARateClassChange(t *testing.T) {
	m := newIsolationManager(2, 2, 1, 0)
	reconcileAll(m, 100, "a") // tier 1, streak=1
	advanceMember(t, m, "a", 400)

	reconcileAll(m, 100, "a") // streak=2 -> tier 2
	require.Equal(t, 2, m.NamespaceTier("a"))
	snap, _ := findMember(m, "a")
	require.Equal(t, int64(400), snap.cursor) // demotion moves no data: cursor untouched
	require.False(t, admits(snap.scope.Contains, "a", 99))

	// Keep throttling: never demotes past the deepest tier.
	reconcileAll(m, 100, "a")
	reconcileAll(m, 100, "a")
	require.Equal(t, 2, m.NamespaceTier("a"))
}

func TestIsolationManager_MergeBack_GatedOnAppliedWatermark(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	reconcileAll(m, 100, "a")
	m.AdvanceDefaultCursor(200)

	// Calm, but the lane has only applied up to 100 while the shared lane resumes
	// from 200: graduating now would leave a gap, so it must not.
	m.Reconcile(nil, 200, map[string]int64{"a": 100})
	require.Equal(t, 1, m.NamespaceTier("a"))
	require.Empty(t, m.PopRetired())

	// Once the lane has applied up to the shared cursor, graduation is gap-free and
	// queues a lane-retirement marker.
	m.Reconcile(nil, 200, map[string]int64{"a": 200})
	require.Equal(t, 0, m.NamespaceTier("a"))
	require.Equal(t, []string{"a"}, m.PopRetired())
	require.Empty(t, m.PopRetired()) // popped once
	require.Nil(t, m.DefaultFilter())
}

func TestIsolationManager_MergeBack_RespectsCooldownAndRethrottleReset(t *testing.T) {
	m := newIsolationManager(2, 5, 2, 0)
	m.AdvanceDefaultCursor(100)
	reconcileAll(m, 100, "a")

	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=1
	require.Equal(t, 1, m.NamespaceTier("a"))
	reconcileAll(m, 100, "a")                         // re-throttled -> calm streak reset
	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=1 again
	require.Equal(t, 1, m.NamespaceTier("a"))
	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // calm=2 -> graduate
	require.Equal(t, 0, m.NamespaceTier("a"))
}

// The merge-back gate is meaningless until the shared lane's position is known: with
// defaultCursor unset (0), `acked >= defaultCursor` would be vacuously true and a
// freshly-built manager could graduate a member before any shared send was recorded,
// stranding the lane's window. Never graduate against an unset cursor.
func TestIsolationManager_MergeBack_NeverGraduatesAgainstUnsetSharedCursor(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	reconcileAll(m, 80, "a")

	m.Reconcile(nil, 80, nil) // calm, but defaultCursor is still 0
	require.Equal(t, 1, m.NamespaceTier("a"))
	require.Empty(t, m.PopRetired())

	m.AdvanceDefaultCursor(90)
	m.Reconcile(nil, 90, map[string]int64{"a": 90})
	require.Equal(t, 0, m.NamespaceTier("a"))
	require.Equal(t, []string{"a"}, m.PopRetired())
}

// A namespace re-throttled after graduating but before its retirement marker was
// popped must not have that marker emitted against its NEW lane.
func TestIsolationManager_Resplit_CancelsPendingRetirement(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(100)
	reconcileAll(m, 100, "a")

	m.Reconcile(nil, 100, map[string]int64{"a": 100}) // graduates; marker queued
	require.Equal(t, 0, m.NamespaceTier("a"))

	reconcileAll(m, 150, "a") // re-throttled before PopRetired ran
	require.True(t, m.IsMember("a"))
	require.Empty(t, m.PopRetired(), "pending retirement for a re-split namespace must be cancelled")
}

// A tier loop that finishes a batch from before a graduation must not fast-forward
// the namespace's NEW lane (post re-split) past tasks that lane never sent.
func TestIsolationManager_AdvanceMemberCursor_StaleGenerationIgnored(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(200)
	reconcileAll(m, 200, "a")
	oldSnap, _ := findMember(m, "a")

	m.Reconcile(nil, 200, map[string]int64{"a": 200}) // graduate
	m.PopRetired()
	reconcileAll(m, 210, "a") // re-split: new incarnation at floor 210
	newSnap, _ := findMember(m, "a")
	require.NotEqual(t, oldSnap.generation, newSnap.generation)

	// The old incarnation's in-flight batch completes and reports progress.
	m.AdvanceMemberCursor("a", oldSnap.generation, 700)
	current, _ := findMember(m, "a")
	require.Equal(t, int64(210), current.cursor, "stale-generation advance must be a no-op")

	m.AdvanceMemberCursor("a", newSnap.generation, 300)
	current, _ = findMember(m, "a")
	require.Equal(t, int64(300), current.cursor)
}

// A stale or regressed receiver report must not rewind a lane's applied watermark.
func TestIsolationManager_Reconcile_AckedIsMonotonic(t *testing.T) {
	m := newIsolationManager(2, 5, 3, 0)
	m.AdvanceDefaultCursor(400)
	reconcileAll(m, 100, "a")

	m.Reconcile([]string{"a"}, 100, map[string]int64{"a": 300})
	m.Reconcile([]string{"a"}, 100, map[string]int64{"a": 250}) // regressed report
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1000,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 1000},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 1000},
	}
	state := m.BuildReaderState(attr)
	require.Len(t, state.Scopes, 4)
	require.Equal(t, int64(300), state.Scopes[3].GetRange().GetInclusiveMin().GetTaskId())
}

// Replication task cleanup deletes strictly below Scopes[0] and reads no other
// scope, so Scopes[0] must never run ahead of any member lane's resume position.
func TestIsolationManager_BuildReaderState_ClampsOverallMinToMemberFloors(t *testing.T) {
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
	require.Equal(t, int64(800), state.Scopes[0].GetRange().GetInclusiveMin().GetTaskId(),
		"scope 0 must be clamped to the member's unsent window so cleanup cannot delete it")
	require.Equal(t, int64(5000), state.Scopes[1].GetRange().GetInclusiveMin().GetTaskId())
	require.Equal(t, int64(800), m.MemberResumeFloor())

	// Once the lane catches up, the clamp lifts with it.
	m.Reconcile([]string{"a"}, 5000, map[string]int64{"a": 5000})
	state = m.BuildReaderState(attr)
	require.Equal(t, int64(5000), state.Scopes[0].GetRange().GetInclusiveMin().GetTaskId())
	require.Equal(t, int64(5000), m.MemberResumeFloor())
}

func TestIsolationManager_MemberResumeFloor_ZeroMinimum(t *testing.T) {
	m := newIsolationManagerWithState(2, 5, 1, 0, 5000, []restoredMember{
		{namespaceID: "zero", cursor: 0},
		{namespaceID: "positive", cursor: 800},
	})
	require.Zero(t, m.MemberResumeFloor())
}

// Misconfiguration must clamp, not strand: tierCount < 1 would exclude isolated
// namespaces from the shared lane while running zero tier send loops.
func TestIsolationManager_ConfigClamps(t *testing.T) {
	m := newIsolationManager(0, 0, 0, 0)
	require.Equal(t, 1, m.TierCount())
	m.AdvanceDefaultCursor(100)
	reconcileAll(m, 100, "a")
	require.Equal(t, 1, m.NamespaceTier("a"))
}

func TestIsolationManager_AdvanceMemberCursor_MonotonicAndScoped(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	reconcileAll(m, 100, "a")

	advanceMember(t, m, "a", 300)
	advanceMember(t, m, "a", 200) // never rewinds
	snap, _ := findMember(m, "a")
	require.Equal(t, int64(300), snap.cursor)

	m.AdvanceMemberCursor("ghost", 1, 500) // unknown member: no-op
	require.Equal(t, 0, m.NamespaceTier("ghost"))
}

func TestIsolationManager_PersistenceRoundTrip(t *testing.T) {
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
	require.Len(t, m.TierMembers(1), 2)

	readerState := m.BuildReaderState(attr)
	require.Len(t, readerState.Scopes, 5) // 3 shared + one per member
	require.Equal(t, int64(1000), readerState.Scopes[0].Range.InclusiveMin.TaskId)
	require.Equal(t, enumsspb.PREDICATE_TYPE_NOT, readerState.Scopes[1].Predicate.PredicateType)
	require.Equal(t, int64(6000), readerState.Scopes[1].Range.InclusiveMin.TaskId)
	require.Equal(t, enumsspb.PREDICATE_TYPE_UNIVERSAL, readerState.Scopes[2].Predicate.PredicateType)

	defaultCursor, restored, err := parseIsolationState(readerState)
	require.NoError(t, err)
	require.Equal(t, int64(6000), defaultCursor)
	require.Len(t, restored, 2)
	byNS := map[string]int64{}
	names := make([]string, 0, len(restored))
	for _, r := range restored {
		byNS[r.namespaceID] = r.cursor
		names = append(names, r.namespaceID)
	}
	require.Equal(t, []string{"a", "b"}, sorted(names))
	require.Equal(t, int64(4000), byNS["a"]) // no ack yet: resumes at floor
	require.Equal(t, int64(2000), byNS["b"]) // resumes at the lane's applied watermark

	// Reconstructing reproduces membership, floors, and cursors; severity resets.
	rebuilt := newIsolationManagerWithState(3, 5, 1, 0, defaultCursor, restored)
	require.Equal(t, 1, rebuilt.NamespaceTier("a"))
	require.Equal(t, 1, rebuilt.NamespaceTier("b"))
	snapB, _ := findMember(rebuilt, "b")
	require.Equal(t, int64(2000), snapB.cursor)
	require.False(t, admits(rebuilt.DefaultFilter(), "a", 9999))
	require.True(t, admits(rebuilt.DefaultFilter(), "healthy", 9999))
}

// A lane watermark reported in the same ack that splits the namespace anchors the
// new member immediately (e.g. a restored lane the receiver is still tracking),
// rather than waiting an extra ack cycle.
func TestIsolationManager_Split_ConsumesReportedWatermark(t *testing.T) {
	m := newIsolationManager(2, 5, 1, 0)
	m.AdvanceDefaultCursor(100)

	m.Reconcile([]string{"a"}, 80, map[string]int64{"a": 95})
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 80,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 80},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 80},
	}
	state := m.BuildReaderState(attr)
	require.Equal(t, int64(95), state.Scopes[3].GetRange().GetInclusiveMin().GetTaskId())
}

// The recv loop reconciles while tier send loops snapshot members and advance
// cursors; drive both concurrently so the race detector verifies the locking.
func TestIsolationManager_ConcurrentReconcileAndAdvance(t *testing.T) {
	m := newIsolationManager(3, 2, 2, 0)
	attr := &replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 1,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 1},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 1},
	}
	var wg sync.WaitGroup
	for g := range 4 {
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

func TestIsolationManager_ParseIsolationState_LegacyScopesIgnored(t *testing.T) {
	// A 3-scope state (no isolation) restores no members.
	m := newIsolationManager(2, 5, 1, 0)
	readerState := m.BuildReaderState(&replicationspb.SyncReplicationState{
		InclusiveLowWatermark: 100,
		HighPriorityState:     &replicationspb.ReplicationState{InclusiveLowWatermark: 200},
		LowPriorityState:      &replicationspb.ReplicationState{InclusiveLowWatermark: 150},
	})
	require.Len(t, readerState.Scopes, 3)
	defaultCursor, restored, err := parseIsolationState(readerState)
	require.NoError(t, err)
	require.Equal(t, int64(200), defaultCursor)
	require.Empty(t, restored)
}

// A scope 3+ that is not a single-namespace member lane was not written by this
// codec; reject it rather than silently dropping a member (which would lift the
// scope-0 cleanup clamp over its unsent window).
func TestIsolationManager_ParseIsolationState_MalformedScopeError(t *testing.T) {
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
	require.ErrorContains(t, err, "unexpected predicate")
}
