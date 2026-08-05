package migration

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestRunEntry_JSONRoundTrip pins down the tuple-array wire shape:
// 1-element when ArchetypeID is zero, 2-element otherwise.
func TestRunEntry_JSONRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		entry  RunEntry
		expect string
	}{
		{"zero archetype is omitted", RunEntry{RunID: "r1"}, `["r1"]`},
		{"non-zero archetype is included", RunEntry{RunID: "r1", ArchetypeID: 42}, `["r1",42]`},
		{"escaped runID", RunEntry{RunID: `r"1`}, `["r\"1"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b, err := json.Marshal(tt.entry)
			require.NoError(t, err)
			require.Equal(t, tt.expect, string(b))

			var out RunEntry
			require.NoError(t, json.Unmarshal(b, &out))
			require.Equal(t, tt.entry, out)
		})
	}
}

// TestRunEntry_UnmarshalRejectsBadShape: the marshaller emits 1- or
// 2-element tuples only; anything else is a protocol violation and
// must surface a clear error rather than a silent zero value.
func TestRunEntry_UnmarshalRejectsBadShape(t *testing.T) {
	cases := []string{
		`[]`,
		`["r1", 1, 2]`,
		`{"r": "r1"}`,
		`"r1"`,
	}
	for _, in := range cases {
		t.Run(in, func(t *testing.T) {
			var out RunEntry
			require.Error(t, json.Unmarshal([]byte(in), &out))
		})
	}
}

// TestBatchPayload_JSONRoundTrip exercises the nested wire shape end
// to end so a change to either RunEntry or the surrounding map type
// can't silently regress it.
func TestBatchPayload_JSONRoundTrip(t *testing.T) {
	p := BatchPayload{
		7: {"bid-a": {{RunID: "r1"}, {RunID: "r2", ArchetypeID: 5}}},
		8: {"bid-b": {{RunID: "r3"}}},
	}
	b, err := json.Marshal(p)
	require.NoError(t, err)
	// Go's encoding/json sorts map keys, so this output is stable.
	require.JSONEq(t, `{
		"7": {"bid-a": [["r1"], ["r2", 5]]},
		"8": {"bid-b": [["r3"]]}
	}`, string(b))

	var out BatchPayload
	require.NoError(t, json.Unmarshal(b, &out))
	require.Equal(t, p, out)
}

// TestBatchPayload_Flatten orders by shard ascending then BID
// alphabetical; runs within a BID keep input order. The activity
// inner loop depends on this for deterministic replays.
func TestBatchPayload_Flatten(t *testing.T) {
	p := BatchPayload{
		2: {"b-z": {{RunID: "rz"}}, "b-a": {{RunID: "ra1"}, {RunID: "ra2"}}},
		1: {"b-c": {{RunID: "rc"}}},
	}
	got := p.flatten()
	require.Len(t, got, 4)
	require.Equal(t, int32(1), got[0].Shard)
	require.Equal(t, "b-c", got[0].BusinessID)
	require.Equal(t, int32(2), got[1].Shard)
	require.Equal(t, "b-a", got[1].BusinessID)
	require.Equal(t, "ra1", got[1].RunID)
	require.Equal(t, "ra2", got[2].RunID)
	require.Equal(t, "b-z", got[3].BusinessID)
}

// TestShardVerifyTracker_FirstVerificationDoubledWindow pins the
// no-progress backstop's grace for a shard's first verified outcome: a
// shard that hasn't verified anything yet gets 2×threshold before
// pickStuck reports it (the server may still be clearing a backlog that
// predates our task submission), then reverts to the plain threshold —
// measured from the verification time — once its first exec verifies.
func TestShardVerifyTracker_FirstVerificationDoubledWindow(t *testing.T) {
	const threshold = 5 * time.Minute
	base := time.Unix(1700000000, 0)

	tr := shardVerifyTracker{0: {pending: 2, lastProgress: base}}

	_, _, stuck := tr.pickStuck(base.Add(threshold+time.Minute), threshold)
	require.False(t, stuck, "must not trip past 1×threshold while awaiting first verification")

	sh, age, stuck := tr.pickStuck(base.Add(2*threshold), threshold)
	require.True(t, stuck, "must trip at 2×threshold while awaiting first verification")
	require.Equal(t, int32(0), sh)
	require.Equal(t, 2*threshold, age)

	// First exec verifies → window reverts to the plain threshold,
	// measured from the verification time.
	verifiedAt := base.Add(threshold)
	tr.recordVerified(0, verifiedAt)

	_, _, stuck = tr.pickStuck(verifiedAt.Add(threshold-time.Second), threshold)
	require.False(t, stuck, "must not trip below 1×threshold after first verification")

	_, _, stuck = tr.pickStuck(verifiedAt.Add(threshold), threshold)
	require.True(t, stuck, "must trip at 1×threshold after first verification")
}

// TestNewShardVerifyTracker_SeedsAllShards: newShardVerifyTracker with
// execs from two shards produces a tracker with entries for each shard,
// correct pending counts, verifiedAny=false, and a seeded lastProgress.
func TestNewShardVerifyTracker_SeedsAllShards(t *testing.T) {
	execs := []*shardedExecutionInfo{
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-0", RunID: "r0"}, Shard: 0},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-1", RunID: "r1"}, Shard: 0},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-2", RunID: "r2"}, Shard: 1},
	}

	before := time.Now()
	tr := newShardVerifyTracker(execs)
	after := time.Now()

	require.Len(t, tr, 2, "should have entries for both shards")

	sv0 := tr[0]
	require.Equal(t, 2, sv0.pending, "shard 0 should have 2 pending execs")
	require.False(t, sv0.verifiedAny, "fresh shard must not have verifiedAny set")
	require.False(t, sv0.lastProgress.IsZero(), "lastProgress must be seeded")
	require.True(t, !sv0.lastProgress.Before(before) && !sv0.lastProgress.After(after),
		"lastProgress should be within the call window")

	sv1 := tr[1]
	require.Equal(t, 1, sv1.pending, "shard 1 should have 1 pending exec")
	require.False(t, sv1.verifiedAny, "fresh shard must not have verifiedAny set")
	require.False(t, sv1.lastProgress.IsZero(), "lastProgress must be seeded")
}

// seedTestExecs builds a fixed five-exec flattened slice spanning shards
// 0 (indices 0,1), 1 (indices 2,3), and 2 (index 4) — the layout shared by
// the seed/heartbeat round-trip tests below.
func seedTestExecs() []*shardedExecutionInfo {
	return []*shardedExecutionInfo{
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-a", RunID: "r0"}, Shard: 0},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-b", RunID: "r1"}, Shard: 0},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-c", RunID: "r2"}, Shard: 1},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-d", RunID: "r3"}, Shard: 1},
		{ExecutionInfo: &ExecutionInfo{BusinessID: "wf-e", RunID: "r4"}, Shard: 2},
	}
}

// TestSeedVerifyState_Fresh: a zero heartbeat (first attempt) yields nothing
// verified and a fresh tracker with full pending counts.
func TestSeedVerifyState_Fresh(t *testing.T) {
	execs := seedTestExecs()
	verified, doneCount, shards := seedVerifyState(execs, replicateBatchHeartbeat{})

	require.Equal(t, []bool{false, false, false, false, false}, verified)
	require.Equal(t, 0, doneCount)
	require.Empty(t, shards.releasedShards())
	require.Equal(t, 2, shards[0].pending)
	require.Equal(t, 2, shards[1].pending)
	require.Equal(t, 1, shards[2].pending)
}

// TestSeedVerifyState_ResumesReleasedAndVerified: a resumed heartbeat marks
// every exec on a released shard verified (without listing them), replays the
// listed per-exec progress, and flags the released shard so it is neither
// re-verified nor re-released.
func TestSeedVerifyState_ResumesReleasedAndVerified(t *testing.T) {
	execs := seedTestExecs()
	hb := replicateBatchHeartbeat{
		InjectDone:     true,
		ReleasedShards: []int32{0},
		VerifiedExecs:  []int{2},
	}

	verified, doneCount, shards := seedVerifyState(execs, hb)

	require.Equal(t, []bool{true, true, true, false, false}, verified)
	require.Equal(t, 3, doneCount)

	// Shard 0 is released: pending drained, released flagged, doneAt cleared so
	// it accrues no idle cost and isn't offered for release again.
	require.Equal(t, 0, shards[0].pending)
	require.True(t, shards[0].released)
	require.True(t, shards[0].doneAt.IsZero())
	require.Equal(t, []int32{0}, shards.releasedShards())
	require.NotContains(t, shards.awaitingRelease(), int32(0), "released shard must not be re-offered")

	// Shard 1 has one of two execs verified; shard 2 is untouched.
	require.Equal(t, 1, shards[1].pending)
	require.True(t, shards[1].verifiedAny)
	require.Equal(t, 1, shards[2].pending)
	require.False(t, shards[2].verifiedAny)
}

// TestSeedVerifyState_IgnoresOutOfRangeIndices: a VerifiedExecs index past the
// exec slice is skipped rather than panicking (defends against tracker/slice
// drift across a resume).
func TestSeedVerifyState_IgnoresOutOfRangeIndices(t *testing.T) {
	execs := seedTestExecs()
	hb := replicateBatchHeartbeat{InjectDone: true, VerifiedExecs: []int{2, 99, -1}}

	verified, doneCount, _ := seedVerifyState(execs, hb)

	require.Equal(t, []bool{false, false, true, false, false}, verified)
	require.Equal(t, 1, doneCount)
}

// TestVerifyHeartbeat_RoundTrip: a heartbeat snapshot omits released shards'
// execs from VerifiedExecs, and seeding from it reconstructs the same verified
// set and done count.
func TestVerifyHeartbeat_RoundTrip(t *testing.T) {
	execs := seedTestExecs()

	// Verify state: shard 0 fully done + released, one exec on shard 1 done.
	verified := []bool{true, true, true, false, false}
	now := time.Now()
	shards := newShardVerifyTracker(execs)
	shards.recordVerified(0, now)
	shards.recordVerified(0, now)
	shards.recordVerified(1, now)
	shards.markReleased([]int32{0})

	hb := verifyHeartbeat(execs, verified, shards)
	require.Equal(t, []int32{0}, hb.ReleasedShards)
	require.Equal(t, []int{2}, hb.VerifiedExecs, "released shard execs must be omitted")
	require.True(t, hb.InjectDone)

	gotVerified, gotDone, _ := seedVerifyState(execs, hb)
	require.Equal(t, verified, gotVerified)
	require.Equal(t, 3, gotDone)
}

// TestEffectiveMaxExecsPerShard: effectiveMaxExecsPerShard returns the full
// MaxExecsPerShard outside a handover phase, and max(cap/2, 1) while in handover.
func TestEffectiveMaxExecsPerShard(t *testing.T) {
	cases := []struct {
		name             string
		handover         bool
		maxExecsPerShard int
		want             int
	}{
		{"not in handover returns full", false, 10, 10},
		{"in handover returns half", true, 10, 5},
		{"minimum of 1 enforced", true, 1, 1},
		{"odd cap rounds down but floors at 1", true, 3, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &shardedWorkflowState{
				params:   &shardedChildParams{MaxExecsPerShard: tc.maxExecsPerShard},
				handover: tc.handover,
			}
			require.Equal(t, tc.want, s.effectiveMaxExecsPerShard())
		})
	}
}

// ---- shardBuckets ----

// requireBucketInvariant asserts shardBuckets' core invariant: counts[sh]
// equals the total runs under byShard[sh], and a shard key is present in both
// maps or in neither (no empty buckets, no orphan counts).
func requireBucketInvariant(t *testing.T, b *shardBuckets) {
	t.Helper()
	for sh, byBID := range b.byShard {
		n := 0
		for _, runs := range byBID {
			n += len(runs)
		}
		require.Greaterf(t, n, 0, "byShard[%d] present but empty", sh)
		require.Equalf(t, n, b.counts[sh], "counts[%d] out of sync with byShard[%d]", sh, sh)
	}
	for sh, c := range b.counts {
		require.Greaterf(t, c, 0, "counts[%d] present but non-positive", sh)
		_, ok := b.byShard[sh]
		require.Truef(t, ok, "counts[%d] present with no byShard entry", sh)
	}
}

// TestShardBuckets_ZeroValueUsable: the zero value answers reads without
// panicking, and add lazily initialises the maps.
func TestShardBuckets_ZeroValueUsable(t *testing.T) {
	var b shardBuckets
	require.True(t, b.empty())
	require.Equal(t, 0, b.count(3))
	require.Equal(t, 0, b.totalRemaining())
	require.Nil(t, b.take(3, 5))
	require.Empty(t, b.shards())

	b.add(3, "bid", RunEntry{RunID: "r1"})
	require.False(t, b.empty())
	require.Equal(t, 1, b.count(3))
	requireBucketInvariant(t, &b)
}

// TestShardBuckets_AddAndCount: add bumps the per-shard count across BIDs and
// shards; totalRemaining sums them and shards lists non-empty shards sorted.
func TestShardBuckets_AddAndCount(t *testing.T) {
	var b shardBuckets
	b.add(1, "a", RunEntry{RunID: "r1"})
	b.add(1, "a", RunEntry{RunID: "r2"})
	b.add(1, "b", RunEntry{RunID: "r3"})
	b.add(2, "c", RunEntry{RunID: "r4"})

	require.Equal(t, 3, b.count(1))
	require.Equal(t, 1, b.count(2))
	require.Equal(t, 0, b.count(99))
	require.Equal(t, 4, b.totalRemaining())
	require.False(t, b.empty())
	require.Equal(t, []int32{1, 2}, b.shards())
	requireBucketInvariant(t, &b)
}

// TestShardBuckets_TakePartial: take walks BIDs alphabetically and takes whole
// per-BID groups only as far as needed to reach n, leaving the remainder intact.
func TestShardBuckets_TakePartial(t *testing.T) {
	var b shardBuckets
	b.add(1, "a", RunEntry{RunID: "r1"})
	b.add(1, "a", RunEntry{RunID: "r2"})
	b.add(1, "a", RunEntry{RunID: "r3"})
	b.add(1, "b", RunEntry{RunID: "r4"})
	b.add(1, "b", RunEntry{RunID: "r5"})
	b.add(1, "c", RunEntry{RunID: "r6"})

	// n=4: take all of "a" (3), then 1 of "b"; stop before "c".
	got := b.take(1, 4)
	require.Equal(t, map[string][]RunEntry{
		"a": {{RunID: "r1"}, {RunID: "r2"}, {RunID: "r3"}},
		"b": {{RunID: "r4"}},
	}, got)
	require.Equal(t, 2, b.count(1)) // r5, r6 remain
	requireBucketInvariant(t, &b)

	// Next take drains the rest, still in BID order.
	got = b.take(1, 100)
	require.Equal(t, map[string][]RunEntry{
		"b": {{RunID: "r5"}},
		"c": {{RunID: "r6"}},
	}, got)
	require.True(t, b.empty())
}

// TestShardBuckets_TakeAllDropsShard: taking everything (or more) removes the
// shard from both maps so empty/shards/count agree.
func TestShardBuckets_TakeAllDropsShard(t *testing.T) {
	var b shardBuckets
	b.add(5, "a", RunEntry{RunID: "r1"})
	b.add(5, "b", RunEntry{RunID: "r2"})

	got := b.take(5, 10) // more than present
	require.Len(t, got, 2)
	require.Equal(t, 0, b.count(5))
	require.True(t, b.empty())
	require.Empty(t, b.shards())

	_, okPayload := b.byShard[5]
	require.False(t, okPayload, "byShard must drop emptied shard")
	_, okCount := b.counts[5]
	require.False(t, okCount, "counts must drop emptied shard")
}

// TestShardBuckets_TakeEdgeCases: non-positive n and empty/missing shards
// return nil without mutating state.
func TestShardBuckets_TakeEdgeCases(t *testing.T) {
	var b shardBuckets
	b.add(1, "a", RunEntry{RunID: "r1"})

	require.Nil(t, b.take(1, 0))
	require.Nil(t, b.take(1, -3))
	require.Nil(t, b.take(99, 5)) // no such shard
	require.Equal(t, 1, b.count(1), "edge-case takes must not mutate")
	requireBucketInvariant(t, &b)
}

// TestShardBuckets_TakeOutputIndependentBacking: the returned slice has its
// own backing array, so appending to it cannot corrupt the bucket's leftover
// runs. Guards the append([]RunEntry(nil), ...) copy in take.
func TestShardBuckets_TakeOutputIndependentBacking(t *testing.T) {
	var b shardBuckets
	b.add(1, "a", RunEntry{RunID: "r1"})
	b.add(1, "a", RunEntry{RunID: "r2"})
	b.add(1, "a", RunEntry{RunID: "r3"})

	got := b.take(1, 1) // takes r1, leaves r2,r3
	require.Equal(t, []RunEntry{{RunID: "r1"}}, got["a"])

	// Appending to the returned slice must not overwrite the leftover r2/r3.
	got["a"] = append(got["a"], RunEntry{RunID: "x"})

	rest := b.take(1, 100)
	require.Equal(t, []RunEntry{{RunID: "r2"}, {RunID: "r3"}}, rest["a"])
}

// ---- inFlightBatches ----

// requireBatchInvariant asserts inFlightBatches' core invariant: inFlight is
// exactly the union of every batch's held set, and each shard is held by at
// most one batch.
func requireBatchInvariant(t *testing.T, b *inFlightBatches) {
	t.Helper()
	holders := map[int32]int{}
	for _, held := range b.held {
		for sh := range held {
			holders[sh]++
		}
	}
	for sh, n := range holders {
		require.Equalf(t, 1, n, "shard %d held by %d batches (must be exactly 1)", sh, n)
		require.Truef(t, b.inFlight[sh], "shard %d held but missing from inFlight", sh)
	}
	require.Equalf(t, len(holders), len(b.inFlight),
		"inFlight (%d entries) must match the union of held sets (%d shards)", len(b.inFlight), len(holders))
}

// payloadForShards builds a minimal BatchPayload claiming the given shards.
// claim only inspects the top-level shard keys, so the inner runs are filler.
func payloadForShards(shards ...int32) BatchPayload {
	p := BatchPayload{}
	for _, sh := range shards {
		p[sh] = map[string][]RunEntry{"b": {{RunID: "r"}}}
	}
	return p
}

// TestInFlightBatches_ZeroValueUsable: reads on the zero value are safe, and
// releasing unknown batches is a no-op; claim lazily initialises the maps.
func TestInFlightBatches_ZeroValueUsable(t *testing.T) {
	var b inFlightBatches
	require.False(t, b.isInFlight(5))
	b.releaseShards(123, []int32{5}) // unknown batch: no-op, no panic
	b.releaseAll(123)                // unknown batch: no-op, no panic

	id := b.claim(payloadForShards(1, 2))
	require.Equal(t, int64(1), id)
	require.True(t, b.isInFlight(1))
	require.True(t, b.isInFlight(2))
	requireBatchInvariant(t, &b)
}

// TestInFlightBatches_ClaimAssignsMonotonicIDs: each claim gets the next ID
// and marks exactly its shards in-flight.
func TestInFlightBatches_ClaimAssignsMonotonicIDs(t *testing.T) {
	var b inFlightBatches
	require.Equal(t, int64(1), b.claim(payloadForShards(1)))
	require.Equal(t, int64(2), b.claim(payloadForShards(2)))
	require.Equal(t, int64(3), b.claim(payloadForShards(3)))
	require.True(t, b.isInFlight(1))
	require.True(t, b.isInFlight(2))
	require.True(t, b.isInFlight(3))
	require.False(t, b.isInFlight(4))
	requireBatchInvariant(t, &b)
}

// TestInFlightBatches_Count: count tracks claimed-but-not-released batches —
// the number of live dispatch coroutines. A batch that has signal-released all
// its shards still counts until releaseAll, because its activity is still
// running.
func TestInFlightBatches_Count(t *testing.T) {
	var b inFlightBatches
	require.Equal(t, 0, b.count())

	b1 := b.claim(payloadForShards(1, 2))
	require.Equal(t, 1, b.count())
	b2 := b.claim(payloadForShards(3))
	require.Equal(t, 2, b.count())

	// Signal-releasing every shard of b1 does NOT drop the batch: the
	// activity is still running, so it stays counted.
	b.releaseShards(b1, []int32{1, 2})
	require.Equal(t, 2, b.count(), "fully signal-released batch still counts until releaseAll")

	b.releaseAll(b1)
	require.Equal(t, 1, b.count())
	b.releaseAll(b2)
	require.Equal(t, 0, b.count())
}

// TestInFlightBatches_ReleaseAll: releaseAll frees the batch's shards and
// drops the batch.
func TestInFlightBatches_ReleaseAll(t *testing.T) {
	var b inFlightBatches
	id := b.claim(payloadForShards(1, 2))
	b.releaseAll(id)

	require.False(t, b.isInFlight(1))
	require.False(t, b.isInFlight(2))
	_, ok := b.held[id]
	require.False(t, ok, "released batch must be dropped from held")
	requireBatchInvariant(t, &b)
}

// TestInFlightBatches_ReleaseShardsPartial: releaseShards frees only the named
// shards; the rest stay claimed until releaseAll.
func TestInFlightBatches_ReleaseShardsPartial(t *testing.T) {
	var b inFlightBatches
	id := b.claim(payloadForShards(1, 2, 3))

	b.releaseShards(id, []int32{2})
	require.True(t, b.isInFlight(1))
	require.False(t, b.isInFlight(2))
	require.True(t, b.isInFlight(3))
	requireBatchInvariant(t, &b)

	b.releaseAll(id)
	require.False(t, b.isInFlight(1))
	require.False(t, b.isInFlight(3))
	requireBatchInvariant(t, &b)
}

// TestInFlightBatches_ReleaseShardsSkipsUnheld: shards not held by the batch
// are ignored (never claimed, or already released — releasing twice is safe).
func TestInFlightBatches_ReleaseShardsSkipsUnheld(t *testing.T) {
	var b inFlightBatches
	id := b.claim(payloadForShards(1))

	// 2 and 3 were never claimed by this batch — releasing them is a no-op
	// and must not disturb shard 1.
	b.releaseShards(id, []int32{2, 3})
	require.True(t, b.isInFlight(1))
	require.False(t, b.isInFlight(2))
	requireBatchInvariant(t, &b)

	// Releasing the same shard twice is harmless.
	b.releaseShards(id, []int32{1})
	b.releaseShards(id, []int32{1})
	require.False(t, b.isInFlight(1))
	requireBatchInvariant(t, &b)
}

// TestInFlightBatches_ReleaseAllDoesNotStompReclaimedShard pins the
// signal-vs-defer invariant: after a batch signal-releases a shard that a
// later batch then re-claims, the first batch's releaseAll must clear only its
// own remaining shards, never the new claimant's.
func TestInFlightBatches_ReleaseAllDoesNotStompReclaimedShard(t *testing.T) {
	var b inFlightBatches

	b1 := b.claim(payloadForShards(1, 2)) // b1 holds {1,2}
	b.releaseShards(b1, []int32{1})       // b1 signal-releases shard 1
	require.False(t, b.isInFlight(1))

	b2 := b.claim(payloadForShards(1)) // b2 re-claims shard 1
	require.True(t, b.isInFlight(1))
	requireBatchInvariant(t, &b)

	// b1's activity returns and clears its remaining claim (shard 2 only).
	b.releaseAll(b1)
	require.True(t, b.isInFlight(1), "shard 1 must stay claimed by b2")
	require.False(t, b.isInFlight(2), "b1's remaining shard 2 must be freed")
	requireBatchInvariant(t, &b)

	// b2 finishes; everything drains.
	b.releaseAll(b2)
	require.False(t, b.isInFlight(1))
	require.Empty(t, b.inFlight)
	require.Empty(t, b.held)
}
