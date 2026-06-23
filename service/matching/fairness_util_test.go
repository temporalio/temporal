package matching

import (
	"hash/maphash"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/service/matching/counter"
)

func TestMergeFairnessWeightOverrides(t *testing.T) {
	t.Run("apply upserts and deletes", func(t *testing.T) {
		existing := fairnessWeightOverrides{"a": 1.0, "b": 2.0}
		set := fairnessWeightOverrides{
			"a": 3.0, // update
			"c": 4.0, // insert
		}
		unset := []string{"b", "x"} // delete existing b and non-existent x (no-op)

		out, err := mergeFairnessWeightOverrides(existing, set, unset, 10)
		require.NoError(t, err)
		require.Equal(t, fairnessWeightOverrides{"a": 3.0, "c": 4.0}, out)
	})

	t.Run("no update", func(t *testing.T) {
		// nil set and unset
		existing := fairnessWeightOverrides{"a": 1.2, "b": 3.4}
		out, err := mergeFairnessWeightOverrides(existing, nil, nil, 10)
		require.NoError(t, err)
		require.Equal(t, existing, out)

		// empty set and unset
		existing = fairnessWeightOverrides{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, fairnessWeightOverrides{}, []string{}, 10)
		require.NoError(t, err)
		require.Equal(t, existing, out)

		// non-existent key in unset
		existing = fairnessWeightOverrides{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, fairnessWeightOverrides{}, []string{"does-not-exist"}, 2)
		require.NoError(t, err)
		require.Equal(t, existing, out)

		// same key and value
		existing = fairnessWeightOverrides{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, fairnessWeightOverrides{"a": 1.2}, []string{}, 2)
		require.NoError(t, err)
		require.Equal(t, existing, out)
	})

	t.Run("return set when existing is empty", func(t *testing.T) {
		set := fairnessWeightOverrides{"a": 1.2, "b": 3.4}

		out, err := mergeFairnessWeightOverrides(nil, set, nil, 10)
		require.NoError(t, err)
		require.Equal(t, set, out)

		out, err = mergeFairnessWeightOverrides(fairnessWeightOverrides{}, set, nil, 10)
		require.NoError(t, err)
		require.Equal(t, set, out)
	})

	t.Run("enforce capacity", func(t *testing.T) {
		existing := fairnessWeightOverrides{"e": 1.0, "b": 2.0}
		unset := []string{"b"}                             // remove one first -> size becomes 1
		set := fairnessWeightOverrides{"a": 3.0, "c": 4.0} // adding two makes size 3 which exceeds capacity 2
		out, err := mergeFairnessWeightOverrides(existing, set, unset, 2)
		require.ErrorIs(t, err, errFairnessOverridesUpdateRejected)
		require.Nil(t, out)
	})

	t.Run("check capacity after deletes", func(t *testing.T) {
		existing := fairnessWeightOverrides{"a": 1.0, "b": 2.0}
		unset := []string{"b"}                   // first, delete one
		set := fairnessWeightOverrides{"c": 5.0} // then, add one
		out, err := mergeFairnessWeightOverrides(existing, set, unset, 2)
		require.NoError(t, err)
		require.Equal(t, fairnessWeightOverrides{"a": 1.0, "c": 5.0}, out)
	})
}

func TestDitherPass(t *testing.T) {
	seed := maphash.MakeSeed()
	const base, inc = 1_000_000, 2000

	t.Run("stays within initial stride", func(t *testing.T) {
		for i := range 10_000 {
			key := string(rune('a'+i%26)) + string(rune('0'+i/26%10)) + string(rune(i))
			p := ditherPass(seed, key, base, inc)
			require.GreaterOrEqual(t, p, int64(base))
			require.Less(t, p, int64(base+inc))
		}
	})

	t.Run("deterministic for a given key and seed", func(t *testing.T) {
		p1 := ditherPass(seed, "key-1", base, inc)
		p2 := ditherPass(seed, "key-1", base, inc)
		require.Equal(t, p1, p2)
	})

	t.Run("spreads distinct keys across the stride", func(t *testing.T) {
		// With many keys we should land in more than a handful of distinct buckets.
		buckets := map[int64]struct{}{}
		for i := range 1000 {
			p := ditherPass(seed, "spread-"+string(rune(i)), base, inc)
			buckets[(p-base)/100] = struct{}{}
		}
		require.Greater(t, len(buckets), 10)
	})

	t.Run("inc of 1 (max weight) does not panic and yields base", func(t *testing.T) {
		require.Equal(t, int64(base), ditherPass(seed, "anything", base, 1))
	})

	t.Run("only fresh/reset keys are moved, established keys untouched", func(t *testing.T) {
		// Mirror pickPasses: dither the base, then ask the counter for the pass. A fresh key
		// lands in the dithered stride; once established, subsequent passes step by inc from
		// its own history and ignore the (lower) dithered base.
		cntr := counter.NewMapCounter(100)
		key := "established"

		first := cntr.GetPass(key, ditherPass(seed, key, base, inc), inc)
		require.GreaterOrEqual(t, first, int64(base))
		require.Less(t, first, int64(base+inc))

		// Next task for the same key steps exactly one stride past the first, regardless of
		// what the dithered base would have been.
		second := cntr.GetPass(key, ditherPass(seed, key, base, inc), inc)
		require.Equal(t, first+inc, second)
	})
}
