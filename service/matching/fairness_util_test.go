package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
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
