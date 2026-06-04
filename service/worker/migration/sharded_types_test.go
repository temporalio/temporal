package migration

import (
	"encoding/json"
	"testing"

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
