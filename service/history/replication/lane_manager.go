package replication

import (
	"math"
	"sync"
)

// laneManager owns the HIGH-priority severity-tier grouping on the replication
// stream sender. The HIGH lane (live replication) is split across a fixed set of
// cursors:
//
//   - the default lane, carrying every non-throttled namespace at full speed; and
//   - tierCount throttled tiers (1..tierCount), carrying progressively more
//     persistent offenders at progressively lower send rates.
//
// A namespace the receiver reports as throttled is first split into tier 1. If it
// stays throttled for demotionCycles consecutive acks it is demoted one tier
// deeper, capped at the deepest tier (the bounding property: cursor/lane count is
// always 1+tierCount regardless of how many namespaces misbehave). When the
// receiver stops reporting it, after cooldownCycles calm acks it graduates straight
// back to the default lane — but only once its tier has *applied* everything up to
// the default lane's cursor (see merge-back below).
//
// # HIGH ordering and applied-watermark floors
//
// HIGH carries ordered history events, so a single workflow's events must never
// straddle two independently-applied lanes: if a fast lane applies a later batch
// before a slower lane delivers an earlier one, the receiver sees a gap and pulls
// the missing range via NDC resend. To avoid that, every lane transition anchors on
// an *applied* (acked) watermark, not a sent cursor: split floors a namespace at the
// default-HIGH acked watermark, and demote floors it at the source tier's acked
// watermark. Everything below the floor is already applied on the receiver, so the
// target lane resumes with no gap; the target re-sends the bounded [acked, sent)
// window, which double-applies idempotently (ErrDuplicate -> success).
//
// # Read cursors and per-namespace floors
//
// Each lane has a read cursor: the task id from which it scans. Each namespace has a
// floor: the task id from which its current lane owns it (the acked watermark of the
// lane it left). A lane sends a member's task T only when T >= floor[ns], so a
// namespace joining a lagging tier is not re-sent below its floor — the tier skips
// it until its scan reaches the floor. One shared scan per lane is preserved.
//
// # Merge-back (best-effort, gap-free)
//
// A calm namespace graduates back to the default lane only once its tier's acked
// watermark has reached the default cursor: the tier has then applied everything the
// default lane is about to resume from, so no workflow straddles the boundary. On a
// continuously-busy shard a rate-limited deep tier may never satisfy this, so
// merge-back is *best-effort* — but staying isolated is harmless for a calm
// namespace (its tier rate limit is not binding, so it still replicates in real
// time), and a stream reconnect (any deploy) resets isolation as a backstop. This is
// the inverse of a LOW-lane trade: HIGH prefers no resend churn over prompt
// hand-back, because a gap costs an NDC resend while lingering isolation costs
// nothing when the namespace is calm.
//
// # Concurrency
//
// laneManager is safe for concurrent use. Reconcile runs on the stream recv loop;
// each lane's send loop calls {Default,Tier}BatchStart for a consistent
// (cursor, filter) snapshot, sends, then Advance*Cursor to commit progress. Tier
// advances use compare-and-swap on the snapshot cursor so a concurrent Reconcile
// rewind is never lost — a failed CAS tells the send loop to re-read and re-send.
type laneManager struct {
	mu             sync.RWMutex
	tierCount      int
	demotionCycles int
	cooldownCycles int

	defaultCursor int64
	tiers         []*laneState            // tiers[t-1] is tier t (1-based)
	namespaces    map[string]*nsLaneState // only holds throttled / in-tier namespaces
}

type laneState struct {
	// cursor is the read cursor; an empty tier holds math.MaxInt64 so it never
	// drags EffectiveLowCursor and is naturally seeded on the next split/demote.
	cursor  int64
	members map[string]struct{}
}

type nsLaneState struct {
	tier            int   // 1..tierCount
	floor           int64 // task id from which the owning lane sends this namespace
	throttledStreak int   // consecutive acks throttled (drives demotion)
	calmStreak      int   // consecutive acks not throttled (drives merge-back cooldown)
}

func newLaneManager(tierCount, demotionCycles, cooldownCycles int) *laneManager {
	tiers := make([]*laneState, tierCount)
	for i := range tiers {
		tiers[i] = &laneState{
			cursor:  math.MaxInt64,
			members: make(map[string]struct{}),
		}
	}
	return &laneManager{
		tierCount:      tierCount,
		demotionCycles: demotionCycles,
		cooldownCycles: cooldownCycles,
		tiers:          tiers,
		namespaces:     make(map[string]*nsLaneState),
	}
}

// reconstructedTier carries a tier's persisted membership and resume cursor, used
// to rebuild the lane manager after a stream reconnect.
type reconstructedTier struct {
	tier    int // 1-based
	cursor  int64
	members []string
}

// newLaneManagerWithState builds a lane manager and restores tier membership and
// cursors from persisted state. This keeps throttled namespaces isolated across a
// stream reconnect: they resume on their tiers (from the persisted acked cursor)
// instead of bursting back onto the default lane and forcing a whole-lane re-send.
// Each restored member's floor is the tier's resume cursor (uniform — the live
// per-namespace floors were only an optimization for mid-stream joins).
func newLaneManagerWithState(tierCount, demotionCycles, cooldownCycles int, defaultCursor int64, tiers []reconstructedTier) *laneManager {
	m := newLaneManager(tierCount, demotionCycles, cooldownCycles)
	m.defaultCursor = defaultCursor
	for _, t := range tiers {
		if t.tier < 1 || t.tier > tierCount || len(t.members) == 0 {
			continue
		}
		lane := m.tiers[t.tier-1]
		lane.cursor = t.cursor
		for _, ns := range t.members {
			lane.members[ns] = struct{}{}
			m.namespaces[ns] = &nsLaneState{tier: t.tier, floor: t.cursor}
		}
	}
	return m
}

// Reconcile updates tier membership against the set of namespaces the receiver
// currently wants throttled. Called once per ack on the recv loop.
//
// highAcked is the default-HIGH lane's acked watermark (the split floor);
// tierAcked[t-1] is tier t's acked watermark (the demote floor and merge-back gate).
// Anchoring on acked watermarks is what keeps HIGH transitions gap-free.
func (m *laneManager) Reconcile(throttled []string, highAcked int64, tierAcked []int64) {
	throttledSet := make(map[string]struct{}, len(throttled))
	for _, ns := range throttled {
		throttledSet[ns] = struct{}{}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 1. Update namespaces already isolated in a tier: demote persistent offenders,
	//    graduate namespaces that have gone calm and whose tier has caught up.
	for ns, st := range m.namespaces {
		if _, isThrottled := throttledSet[ns]; isThrottled {
			st.calmStreak = 0
			st.throttledStreak++
			if st.throttledStreak >= m.demotionCycles && st.tier < m.tierCount {
				m.demoteLocked(ns, st, tierAckedAt(tierAcked, st.tier))
			}
		} else {
			st.throttledStreak = 0
			st.calmStreak++
			if st.calmStreak >= m.cooldownCycles {
				m.tryGraduateLocked(ns, st, tierAckedAt(tierAcked, st.tier))
			}
		}
	}

	// 2. Split newly-throttled namespaces into tier 1, floored at the HIGH acked
	//    watermark so the tier resumes from an already-applied point (no gap).
	for ns := range throttledSet {
		if _, exists := m.namespaces[ns]; !exists {
			m.splitLocked(ns, highAcked)
		}
	}
}

// tierAckedAt returns tier's acked watermark, or math.MinInt64 when it is not
// reported (so a floor/gate that reads it stays conservative — never spuriously
// ahead of the default cursor).
func tierAckedAt(tierAcked []int64, tier int) int64 {
	if tier-1 < 0 || tier-1 >= len(tierAcked) {
		return math.MinInt64
	}
	return tierAcked[tier-1]
}

// splitLocked moves a namespace from the default lane into tier 1, flooring it at
// the default-HIGH acked watermark: the tier resumes from an already-applied point,
// so no workflow straddles the boundary. The tier re-sends the bounded
// [highAcked, defaultCursor) window it overlaps with the default lane, which
// double-applies idempotently.
func (m *laneManager) splitLocked(ns string, highAcked int64) {
	tier1 := m.tiers[0]
	if tier1.cursor > highAcked {
		// Empty tier (seed) or a tier read ahead of the joiner's floor (rewind to
		// avoid a gap). For a lagging tier this is a no-op.
		tier1.cursor = highAcked
	}
	tier1.members[ns] = struct{}{}
	m.namespaces[ns] = &nsLaneState{tier: 1, floor: highAcked, throttledStreak: 1}
}

// demoteLocked moves a namespace one tier deeper, flooring it at the source tier's
// acked watermark (already applied on the receiver), so the deeper tier resumes with
// no gap even though HIGH events are ordered.
func (m *laneManager) demoteLocked(ns string, st *nsLaneState, srcAcked int64) {
	from := m.tiers[st.tier-1]
	to := m.tiers[st.tier] // tier st.tier+1
	floor := srcAcked
	delete(from.members, ns)
	if len(from.members) == 0 {
		from.cursor = math.MaxInt64
	}
	if to.cursor > floor {
		to.cursor = floor // seed empty target, or rare rewind to avoid a gap
	}
	to.members[ns] = struct{}{}
	st.tier++
	st.floor = floor
	st.throttledStreak = 0 // require another demotionCycles before demoting again
}

// tryGraduateLocked returns a calm namespace to the default lane, but only once its
// tier has *applied* everything up to the default cursor (tierAcked >= defaultCursor).
// The default lane resumes the namespace from its current cursor; because the tier
// has already applied every earlier event, no workflow straddles the hand-off and the
// receiver sees no gap. Best-effort: if the (rate-limited) tier has not caught up,
// the namespace stays isolated — harmless while it is calm.
func (m *laneManager) tryGraduateLocked(ns string, st *nsLaneState, tierAcked int64) {
	if tierAcked < m.defaultCursor {
		return // tier has not applied up to where the default lane would resume
	}
	tier := m.tiers[st.tier-1]
	delete(tier.members, ns)
	if len(tier.members) == 0 {
		tier.cursor = math.MaxInt64
	}
	delete(m.namespaces, ns)
}

// DefaultBatchStart returns a consistent (cursor, filter) snapshot for the default
// HIGH send loop. The filter excludes every namespace currently owned by a tier (task
// id is irrelevant for the default lane — a graduating namespace is removed from the
// tier only once the tier has applied up to this cursor, so resuming here is gap-free).
func (m *laneManager) DefaultBatchStart() (int64, func(namespaceID string, taskID int64) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	excluded := make(map[string]struct{}, len(m.namespaces))
	for ns := range m.namespaces {
		excluded[ns] = struct{}{}
	}
	cursor := m.defaultCursor
	return cursor, func(namespaceID string, _ int64) bool {
		_, isExcluded := excluded[namespaceID]
		return !isExcluded
	}
}

// TierBatchStart returns a consistent (cursor, filter) snapshot for tier's send loop.
// The filter admits a member's task at or above the member's floor, so the tier never
// re-sends already-covered history.
func (m *laneManager) TierBatchStart(tier int) (int64, func(namespaceID string, taskID int64) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t := m.tiers[tier-1]
	floors := make(map[string]int64, len(t.members))
	for ns := range t.members {
		floors[ns] = m.namespaces[ns].floor
	}
	cursor := t.cursor
	return cursor, func(namespaceID string, taskID int64) bool {
		floor, ok := floors[namespaceID]
		return ok && taskID >= floor
	}
}

// AdvanceDefaultCursor records default-lane send progress. The default lane has a
// single writer and is never rewound, so this is a simple monotonic advance.
func (m *laneManager) AdvanceDefaultCursor(to int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if to > m.defaultCursor {
		m.defaultCursor = to
	}
}

// AdvanceTierCursor commits a tier send loop's progress from->to. It is a
// compare-and-swap on the cursor the loop observed at batch start: if Reconcile
// rewound the cursor in the meantime, the CAS fails and the caller must re-read
// and re-send from the new (lower) cursor. Returns true if committed.
func (m *laneManager) AdvanceTierCursor(tier int, from, to int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.tiers[tier-1]
	if t.cursor != from {
		return false
	}
	t.cursor = to
	return true
}

// EffectiveLowCursor is the minimum read cursor across the default lane and all
// non-empty tiers — the lowest point still in flight on the LOW lane, and the basis
// for the cleanup-safe persisted watermark.
func (m *laneManager) EffectiveLowCursor() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	min := m.defaultCursor
	for _, t := range m.tiers {
		if t.cursor < min {
			min = t.cursor
		}
	}
	return min
}

// TierCount returns the number of throttled tiers.
func (m *laneManager) TierCount() int {
	return m.tierCount
}

// DefaultCursor returns the default lane's read cursor.
func (m *laneManager) DefaultCursor() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.defaultCursor
}

// TierCursor returns tier's read cursor (math.MaxInt64 when the tier is empty).
func (m *laneManager) TierCursor(tier int) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.tiers[tier-1].cursor
}

// NamespaceTier returns the tier owning ns (1..tierCount), or 0 if ns is on the
// default lane.
func (m *laneManager) NamespaceTier(ns string) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if st, ok := m.namespaces[ns]; ok {
		return st.tier
	}
	return 0
}

// NamespaceFloor returns the floor of ns and whether ns is currently isolated.
func (m *laneManager) NamespaceFloor(ns string) (int64, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if st, ok := m.namespaces[ns]; ok {
		return st.floor, true
	}
	return 0, false
}

// AllThrottledNamespaces returns every namespace currently owned by a tier. Used
// to build the default lane's exclusion predicate for persistence.
func (m *laneManager) AllThrottledNamespaces() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.namespaces))
	for ns := range m.namespaces {
		out = append(out, ns)
	}
	return out
}

// TierMembers returns the namespaces currently owned by tier. Used to build the
// tier's predicate for persistence.
func (m *laneManager) TierMembers(tier int) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t := m.tiers[tier-1]
	out := make([]string, 0, len(t.members))
	for ns := range t.members {
		out = append(out, ns)
	}
	return out
}
