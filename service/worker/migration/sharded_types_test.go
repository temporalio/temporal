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

// TestEffectiveMaxExecsPerShard: effectiveMaxExecsPerShard returns the
// full MaxExecsPerShard when promoted and not cut, and max(cap/2, 1)
// in all other cases.
func TestEffectiveMaxExecsPerShard(t *testing.T) {
	cases := []struct {
		name             string
		promoted, cut    bool
		maxExecsPerShard int
		want             int
	}{
		{"promoted and not cut returns full", true, false, 10, 10},
		{"not promoted returns half", false, false, 10, 5},
		{"promoted and cut returns half", true, true, 10, 5},
		{"minimum of 1 enforced", false, false, 1, 1},
		{"odd cap rounds down but floors at 1", false, false, 3, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := &shardedWorkflowState{
				params:   &shardedChildParams{MaxExecsPerShard: tc.maxExecsPerShard},
				promoted: tc.promoted,
				cut:      tc.cut,
			}
			require.Equal(t, tc.want, s.effectiveMaxExecsPerShard())
		})
	}
}
