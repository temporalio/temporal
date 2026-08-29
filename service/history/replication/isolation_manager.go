package replication

import (
	"fmt"
	"math"
	"slices"
	"sync"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/predicates"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/tasks"
)

// isolationManager owns the sender-side state of HIGH-lane namespace isolation.
// Each namespace the receiver reports as overwhelming the shared HIGH (live) lane is
// split onto its own dedicated lane: an independent read cursor and wire stream,
// paced by a throttled severity tier (a rate class). Because every member owns its
// cursor, members never interact — joining, demoting, or graduating one namespace
// moves no other namespace's position, and no cursor ever rewinds.
//
// A member starts in tier 1; if it stays throttled for demotionCycles consecutive
// acks it is demoted one tier deeper (just a rate-class change), capped at the
// deepest tier. When the receiver stops reporting it, after cooldownCycles calm acks
// it graduates back to the shared lane — but only once its lane has *applied*
// everything up to the shared lane's cursor, so ordered HIGH events never straddle
// the hand-off with a gap. Graduated members are queued for the send loop to emit a
// lane-retirement marker, letting the receiver drop the lane's tracking.
//
// # Ordering and applied-watermark floors
//
// HIGH carries ordered history events, so a single workflow's events must never
// straddle two independently-applied lanes with a gap. A split floors the member at
// the shared lane's acked watermark: everything below is already applied, the shared
// lane's in-flight window above it is re-sent on the member lane (double-applying
// idempotently), and the member lane owns the namespace from the floor on.
// Graduation is gated the same way in reverse: the member's lane must have applied
// up to the shared cursor before the shared lane resumes the namespace.
//
// # Bounding
//
// The lane count is bounded by maxIsolated (0 = no limit): beyond the cap, newly
// throttled namespaces stay on the shared lane — isolation stops helping, which
// degrades to exactly the un-isolated behavior. This bounds per-namespace state:
// persisted member scopes, per-ack lane reports, and receiver-side lane trackers.
//
// # Concurrency
//
// isolationManager is safe for concurrent use: Reconcile runs on the stream recv
// loop while tier send loops snapshot members and advance their cursors. A member's
// cursor has a single writer (its tier's send loop); Reconcile never moves cursors.
type isolationManager struct {
	mu             sync.Mutex
	tierCount      int
	demotionCycles int
	cooldownCycles int
	maxIsolated    int // max namespaces isolated at once; 0 = no limit

	defaultCursor          int64
	nextGeneration         int64 // monotonic lane-incarnation counter
	members                map[string]*isolatedMember
	retired                []string // graduated namespaces awaiting a lane-retirement marker
	defaultCoverageTarget  int64
	defaultCoveragePending bool
}

// isolatedMember is one namespace's dedicated lane: the scope of tasks the lane owns
// ([floor, MaxInt64) x namespace predicate), its read cursor, the receiver-reported
// applied watermark for the lane, and the ack streaks driving demotion/merge-back.
type isolatedMember struct {
	// generation identifies this incarnation of the namespace's lane. A namespace
	// can graduate and later re-split; state updates carrying a stale generation
	// (e.g. a tier loop finishing a batch from before the graduation) must not be
	// applied to the new incarnation — the classic ABA hazard.
	generation      int64
	tier            int          // severity rate class, 1..tierCount
	scope           queues.Scope // [floor, MaxInt64) x the namespace
	cursor          int64        // lane read (send) position
	acked           int64        // lane applied watermark reported by the receiver (0 = none yet)
	throttledStreak int          // consecutive acks throttled (drives demotion)
	calmStreak      int          // consecutive acks not throttled (drives merge-back cooldown)
}

func (m *isolatedMember) floor() int64 {
	return m.scope.Range.InclusiveMin.TaskID
}

// namespaceScope is the scope a member lane owns: every task of the namespace at or
// above the floor.
func namespaceScope(namespaceID string, floor int64) queues.Scope {
	return queues.NewScope(
		queues.NewRange(tasks.NewImmediateKey(floor), tasks.NewImmediateKey(math.MaxInt64)),
		tasks.NewNamespacePredicate([]string{namespaceID}),
	)
}

func newIsolationManager(tierCount, demotionCycles, cooldownCycles, maxIsolated int) *isolationManager {
	// Clamp misconfiguration rather than letting it strand namespaces: tierCount < 1
	// would exclude isolated members from the shared lane while running zero tier
	// send loops, and non-positive cycle counts would demote or graduate instantly.
	return &isolationManager{
		tierCount:      max(1, tierCount),
		demotionCycles: max(1, demotionCycles),
		cooldownCycles: max(1, cooldownCycles),
		maxIsolated:    maxIsolated,
		members:        make(map[string]*isolatedMember),
	}
}

// restoredMember carries a member lane's persisted resume cursor, used to rebuild
// the manager after a stream reconnect.
type restoredMember struct {
	namespaceID string
	cursor      int64
}

// newIsolationManagerWithState builds an isolation manager and restores member lanes
// from persisted state, so throttled namespaces stay isolated across a stream
// reconnect (e.g. history deploys) instead of bursting back onto the shared lane.
// Restored members resume at their persisted applied watermark and reset to tier 1
// (severity re-escalates from live behavior; a reconnect is a backstop, not a
// continuation). Restored members are kept even above a lowered cap — dropping them
// would gap their lanes; the cap only stops new splits.
func newIsolationManagerWithState(tierCount, demotionCycles, cooldownCycles, maxIsolated int, defaultCursor int64, restored []restoredMember) *isolationManager {
	m := newIsolationManager(tierCount, demotionCycles, cooldownCycles, maxIsolated)
	m.defaultCursor = defaultCursor
	for _, r := range restored {
		m.nextGeneration++
		m.members[r.namespaceID] = &isolatedMember{
			generation: m.nextGeneration,
			tier:       1,
			scope:      namespaceScope(r.namespaceID, r.cursor),
			cursor:     r.cursor,
		}
	}
	return m
}

// Reconcile updates membership against the set of namespaces the receiver currently
// wants throttled. Called once per ack on the recv loop.
//
// highAcked is the shared HIGH lane's acked watermark (the split floor); memberAcked
// holds each member lane's acked watermark (the merge-back gate), keyed by namespace.
func (m *isolationManager) Reconcile(throttled []string, highAcked int64, memberAcked map[string]int64) {
	throttledSet := make(map[string]struct{}, len(throttled))
	for _, ns := range throttled {
		throttledSet[ns] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Update isolated members: record lane applied watermarks, demote persistent
	//    offenders one rate class deeper, graduate members that have gone calm and
	//    whose lane has caught up to the shared cursor.
	for ns, st := range m.members {
		if acked, ok := memberAcked[ns]; ok && acked > st.acked {
			// Monotonic: a stale or regressed receiver report must not rewind the
			// lane's applied watermark (it feeds the persisted resume point and the
			// merge-back gate).
			st.acked = acked
		}
		if _, isThrottled := throttledSet[ns]; isThrottled {
			st.calmStreak = 0
			st.throttledStreak++
			if st.throttledStreak >= m.demotionCycles && st.tier < m.tierCount {
				st.tier++
				st.throttledStreak = 0 // require another demotionCycles before demoting again
			}
		} else {
			st.throttledStreak = 0
			st.calmStreak++
			// Merge-back is gated on the lane having *applied* everything up to where
			// the shared lane resumes, so no workflow straddles the hand-off. On a
			// busy shard a deep lane may lag the gate for a while — harmless, the calm
			// member still replicates in real time on its own lane.
			// defaultCursor == 0 means the shared lane's position is not known yet
			// (no shared send has been recorded); the gate would be vacuous, so
			// never graduate against it.
			if st.calmStreak >= m.cooldownCycles && m.defaultCursor > 0 && st.acked >= m.defaultCursor {
				delete(m.members, ns)
				m.retired = append(m.retired, ns)
			}
		}
	}

	// 2. Split newly-throttled namespaces onto their own lanes, floored at the HIGH
	//    acked watermark so the lane starts from an already-applied point (no gap).
	//    The isolated set is capped so membership metadata cannot grow without bound.
	//    Iterate the receiver-reported order rather than the set so that when the cap
	//    bites, which namespaces get lanes is deterministic.
	for _, ns := range throttled {
		if _, exists := m.members[ns]; exists {
			continue
		}
		if m.maxIsolated > 0 && len(m.members) >= m.maxIsolated {
			break
		}
		// A re-split cancels any pending retirement marker for the namespace:
		// emitting it now would retire the NEW lane's receiver-side tracking.
		m.retired = slices.DeleteFunc(m.retired, func(retired string) bool { return retired == ns })
		m.nextGeneration++
		st := &isolatedMember{
			generation:      m.nextGeneration,
			tier:            1,
			scope:           namespaceScope(ns, highAcked),
			cursor:          highAcked,
			throttledStreak: 1,
		}
		// The receiver may already report a watermark for the lane it expects (e.g.
		// right after a restore); don't wait an extra ack cycle to anchor on it.
		if acked, ok := memberAcked[ns]; ok {
			st.acked = acked
		}
		m.members[ns] = st
		m.defaultCoveragePending = false
	}
}

// IsMember reports whether the namespace currently has an isolated lane. The sender
// uses it to drop retirement markers that were queued before a re-split.
func (m *isolationManager) IsMember(namespaceID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.members[namespaceID]
	return ok
}

// IsMemberGeneration reports whether the namespace's isolated lane is still the
// given incarnation. Tier send loops verify their snapshot with this before (and
// again just after) taking a send lease, so a lane incarnation that graduated or
// re-split sends nothing more.
func (m *isolationManager) IsMemberGeneration(namespaceID string, generation int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.members[namespaceID]
	return ok && st.generation == generation
}

// DefaultFilter returns the task filter for the shared HIGH lane: it excludes every
// isolated namespace. Returns nil (admit all) when no namespace is isolated. The
// filter is an immutable snapshot; re-snapshot per batch.
func (m *isolationManager) DefaultFilter() func(task tasks.Task) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.members) == 0 {
		return nil
	}
	predicate := predicates.Not[tasks.Task](tasks.NewNamespacePredicate(m.sortedMembers()))
	return predicate.Test
}

// sortedMembers returns the isolated namespaces in sorted order so filters and
// persisted state are deterministic across calls. Caller must hold m.mu.
func (m *isolationManager) sortedMembers() []string {
	out := make([]string, 0, len(m.members))
	for ns := range m.members {
		out = append(out, ns)
	}
	slices.Sort(out)
	return out
}

// memberSnapshot is a consistent view of one member lane for its tier's send loop.
type memberSnapshot struct {
	namespaceID string
	generation  int64
	cursor      int64
	scope       queues.Scope
}

// TierMemberSnapshots returns a snapshot of every member currently in the given
// severity tier, for that tier's send loop.
func (m *isolationManager) TierMemberSnapshots(tier int) []memberSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []memberSnapshot
	for ns, st := range m.members {
		if st.tier == tier {
			out = append(out, memberSnapshot{namespaceID: ns, generation: st.generation, cursor: st.cursor, scope: st.scope})
		}
	}
	return out
}

// AdvanceMemberCursor records a member lane's send progress. The cursor has a single
// writer per lane incarnation (the member's tier send loop) and never rewinds. The
// generation (from the snapshot the send was made under) guards the ABA case: an
// advance for a member that graduated — even one that has since re-split into a new
// lane — is a no-op, so a stale batch's progress can never fast-forward a new lane's
// cursor past tasks the new lane has not sent.
func (m *isolationManager) AdvanceMemberCursor(namespaceID string, generation int64, to int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.members[namespaceID]; ok && st.generation == generation && to > st.cursor {
		st.cursor = to
	}
}

// PopRetired returns the namespaces that graduated since the last call. The caller
// sends a lane-retirement marker for each, letting the receiver drop the lane.
func (m *isolationManager) PopRetired() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := m.retired
	m.retired = nil
	return out
}

// AdvanceDefaultCursor records shared-lane send progress; the merge-back gate
// compares member lane acked watermarks against it.
func (m *isolationManager) AdvanceDefaultCursor(to int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if to > m.defaultCursor {
		m.defaultCursor = to
	}
}

// DefaultCursor returns the shared HIGH lane's cursor.
func (m *isolationManager) DefaultCursor() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.defaultCursor
}

// TierCount returns the number of severity tiers (rate classes).
func (m *isolationManager) TierCount() int {
	return m.tierCount
}

// NamespaceTier returns the tier of an isolated namespace, or 0 if it is on the
// shared lane.
func (m *isolationManager) NamespaceTier(ns string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st, ok := m.members[ns]; ok {
		return st.tier
	}
	return 0
}

// TierMembers returns the namespaces currently in the given tier.
func (m *isolationManager) TierMembers(tier int) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for ns, st := range m.members {
		if st.tier == tier {
			out = append(out, ns)
		}
	}
	return out
}

// TierMemberCount returns the number of namespaces currently in the given tier.
func (m *isolationManager) TierMemberCount(tier int) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, st := range m.members {
		if st.tier == tier {
			count++
		}
	}
	return count
}

// BuildReaderState builds the persisted reader state: scope 0 = overall min
// (universal), 1 = shared HIGH (predicate excludes isolated namespaces), 2 = LOW
// (universal), 3+ = one scope per isolated member (its namespace predicate, resuming
// at the lane's applied watermark). parseIsolationState is the inverse.
//
// Scope 0 is clamped to the minimum over the member scopes. Replication task cleanup
// deletes strictly below Scopes[0] and looks at no other scope, while the receiver's
// folded watermark only covers lanes that exist on the receiver's current connection
// — after a reconnect (no lanes yet) the fold jumps to the shared minimum even
// though restored members hold unsent windows far below it. Without the clamp,
// cleanup would delete those windows before the tier loops re-deliver them.
func (m *isolationManager) BuildReaderState(attr *replicationspb.SyncReplicationState) *persistencespb.QueueReaderState {
	m.mu.Lock()
	defer m.mu.Unlock()

	scope := func(watermark int64, predicate tasks.Predicate) *persistencespb.QueueSliceScope {
		return queues.ToPersistenceScope(queues.NewScope(
			queues.NewRange(tasks.NewImmediateKey(watermark), tasks.NewImmediateKey(math.MaxInt64)),
			predicate,
		))
	}

	sorted := m.sortedMembers()
	sharedHighPredicate := predicates.Universal[tasks.Task]()
	if len(sorted) > 0 {
		sharedHighPredicate = predicates.Not[tasks.Task](tasks.NewNamespacePredicate(sorted))
	}
	overallMin := attr.GetInclusiveLowWatermark()
	memberScopes := make([]*persistencespb.QueueSliceScope, 0, len(sorted))
	for _, ns := range sorted {
		st := m.members[ns]
		// Resume from the lane's applied watermark; a lane with no ack yet resumes
		// from its floor.
		resume := max(st.floor(), st.acked)
		overallMin = min(overallMin, resume)
		memberScopes = append(memberScopes, scope(resume, tasks.NewNamespacePredicate([]string{ns})))
	}
	scopes := append([]*persistencespb.QueueSliceScope{
		scope(overallMin, predicates.Universal[tasks.Task]()),
		scope(attr.GetHighPriorityState().GetInclusiveLowWatermark(), sharedHighPredicate),
		scope(attr.GetLowPriorityState().GetInclusiveLowWatermark(), predicates.Universal[tasks.Task]()),
	}, memberScopes...)
	return &persistencespb.QueueReaderState{Scopes: scopes}
}

// MemberResumeFloor returns the minimum resume position over all member lanes
// (max(floor, acked) per lane). Until the receiver confirms isolation support, the
// shared lane's catch-up must start no higher than this so restored members' unsent
// windows are covered by the shared lane instead of stranding on tier lanes that
// will never run.
func (m *isolationManager) MemberResumeFloor() (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.members) == 0 {
		return 0, false
	}
	floor := int64(math.MaxInt64)
	for _, st := range m.members {
		floor = min(floor, max(st.floor(), st.acked))
	}
	return floor, true
}

// RecordDefaultLaneCoverage records a successfully-sent unfiltered shared-lane scan
// through the queue's exclusive high watermark.
func (m *isolationManager) RecordDefaultLaneCoverage(beginInclusive, queueExclusiveHigh int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.members) == 0 {
		return false
	}
	floor := int64(math.MaxInt64)
	for _, st := range m.members {
		floor = min(floor, max(st.floor(), st.acked))
	}
	if beginInclusive > floor {
		return false
	}
	// Equality is valid: when the exclusive high watermark equals the member floor,
	// the pending replay range is empty and no task at the floor is readable yet.
	if queueExclusiveHigh < floor {
		return false
	}
	m.defaultCoverageTarget = queueExclusiveHigh
	m.defaultCoveragePending = true
	return true
}

// CollapseToDefaultLane removes all isolated lanes once the receiver has applied a
// recorded unfiltered shared-lane range. Requiring explicit coverage prevents a
// receiver's pre-existing shared watermark from dropping unrestored member windows.
func (m *isolationManager) CollapseToDefaultLane(defaultAcked int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.members) == 0 && len(m.retired) == 0 {
		return false
	}
	if len(m.members) > 0 && (!m.defaultCoveragePending || defaultAcked < m.defaultCoverageTarget) {
		return false
	}
	clear(m.members)
	m.retired = nil
	m.defaultCoveragePending = false
	return true
}

// parseIsolationState extracts the shared-HIGH resume cursor and the member lanes
// from a persisted reader state. Pure; the inverse of BuildReaderState. Scopes 3+
// must all be single-namespace member lanes; anything else means the state was not
// written by this codec, so it is rejected rather than silently dropped — a dropped
// member would lift the scope-0 cleanup clamp over its unsent window.
func parseIsolationState(readerState *persistencespb.QueueReaderState) (int64, []restoredMember, error) {
	if len(readerState.Scopes) < 3 {
		return 0, nil, nil
	}
	defaultCursor := readerState.Scopes[1].GetRange().GetInclusiveMin().GetTaskId()
	var restored []restoredMember
	for i, persistedScope := range readerState.Scopes[3:] {
		scope := queues.FromPersistenceScope(persistedScope)
		nsPredicate, ok := scope.Predicate.(*tasks.NamespacePredicate)
		if !ok || len(nsPredicate.NamespaceIDs) != 1 {
			return 0, nil, fmt.Errorf("isolation member scope %d has unexpected predicate %T", i+3, scope.Predicate)
		}
		for ns := range nsPredicate.NamespaceIDs {
			restored = append(restored, restoredMember{
				namespaceID: ns,
				cursor:      scope.Range.InclusiveMin.TaskID,
			})
		}
	}
	return defaultCursor, restored, nil
}
