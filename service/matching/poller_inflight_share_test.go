package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func tracker(counts map[string]int) *inflightPollTracker {
	t := newInflightPollTracker()
	for k, n := range counts {
		for i := 0; i < n; i++ {
			t.add(k, 1)
		}
	}
	return t
}

func TestInflightClassify(t *testing.T) {
	const band = 1.2

	// Balanced: the caller holds one extra poll because its own is mid-match.
	// Without discounting it, 4 vs a mean of 3.33 would trip the band exactly.
	require.Equal(t, shareFair, tracker(map[string]int{"a": 4, "b": 3, "c": 3}).classify("a", band),
		"a balanced fleet must not flag the worker being served")

	require.Equal(t, shareOver, tracker(map[string]int{"a": 10, "b": 3, "c": 3}).classify("a", band))
	require.Equal(t, shareUnder, tracker(map[string]int{"a": 2, "b": 10, "c": 10}).classify("a", band))

	// Fail-open conditions.
	require.Equal(t, shareUnknown, tracker(map[string]int{"a": 5}).classify("a", band), "no peer")
	require.Equal(t, shareUnknown, tracker(map[string]int{"a": 5, "b": 5}).classify("zz", band), "unknown worker")
	require.Equal(t, shareUnknown, tracker(map[string]int{"a": 5, "b": 5}).classify("", band), "no instance key")
	require.Equal(t, shareUnknown, tracker(map[string]int{"a": 9, "b": 1}).classify("a", 1.0), "band disabled")

	// Entries are dropped at zero so a departed worker stops counting.
	tr := tracker(map[string]int{"a": 1, "b": 5})
	tr.add("a", -1)
	require.Equal(t, shareUnknown, tr.classify("b", band), "lone survivor has no peer")
}
