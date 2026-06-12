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

// TestBatchPayload_mergeInto merges payload runs into dst and keeps
// bucketCounts in sync — the same invariant addToBucket maintains per
// run, but for a bulk restore on CAN entry.
func TestBatchPayload_mergeInto(t *testing.T) {
	dst := BatchPayload{1: {"a": {{RunID: "r0"}}}}
	counts := map[int32]int{1: 1}

	src := BatchPayload{
		1: {"b": {{RunID: "r1"}, {RunID: "r2"}}},
		2: {"c": {{RunID: "r3"}}},
	}
	src.mergeInto(dst, counts)

	require.Len(t, dst, 2)
	require.Len(t, dst[1]["a"], 1)
	require.Len(t, dst[1]["b"], 2)
	require.Len(t, dst[2]["c"], 1)
	require.Equal(t, map[int32]int{1: 3, 2: 1}, counts)
	require.Equal(t, 4, dst.totalRuns())
}

func TestResumeShardPayloadRoundTrip(t *testing.T) {
	shards := []ResumeShard{
		{Shard: 2, Execs: map[string][]RunEntry{"b": {{RunID: "r2"}}}, NoProgressDuration: 3 * time.Second},
		{Shard: 1, Execs: map[string][]RunEntry{"a": {{RunID: "r1"}, {RunID: "r1b"}}}, NoProgressDuration: 5 * time.Second},
	}
	payload, noProgress := resumeShardsToPayload(shards)
	got := resumeShardsFromPayload(payload, noProgress)

	require.Len(t, got, 2)
	require.Equal(t, int32(1), got[0].Shard)
	require.Equal(t, 5*time.Second, got[0].NoProgressDuration)
	require.Len(t, got[0].Execs["a"], 2)
	require.Equal(t, int32(2), got[1].Shard)
	require.Equal(t, 3*time.Second, got[1].NoProgressDuration)
}

func TestResumeShardsToPayload_mergesDuplicateShards(t *testing.T) {
	shards := []ResumeShard{
		{Shard: 1, Execs: map[string][]RunEntry{"a": {{RunID: "r1"}}}, NoProgressDuration: time.Second},
		{Shard: 1, Execs: map[string][]RunEntry{"b": {{RunID: "r2"}}}, NoProgressDuration: 2 * time.Second},
	}
	payload, noProgress := resumeShardsToPayload(shards)
	require.Len(t, payload[1], 2)
	require.Equal(t, 2*time.Second, noProgress[1])
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

// TestNewShardVerifyTracker_ResumeSkipsDoubledWindow: a resumed shard's
// tasks were submitted (and given their first-verification grace) in a
// prior CAN cycle, so it is seeded as already past its first
// verification and tracks cumulative no-progress against the plain
// threshold. A fresh shard awaits its first verification.
func TestNewShardVerifyTracker_ResumeSkipsDoubledWindow(t *testing.T) {
	execs := []*shardedExecutionInfo{{Shard: 0}}

	fresh := newShardVerifyTracker(execs, false, nil)
	require.False(t, fresh[0].verifiedAny, "fresh shard awaits its first verification")

	resumed := newShardVerifyTracker(execs, true, map[int32]time.Duration{0: time.Minute})
	require.True(t, resumed[0].verifiedAny, "resumed shard skips the doubled first-verification window")
}
