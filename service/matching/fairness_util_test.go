package matching

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMergeFairnessWeightOverrides(t *testing.T) {
	t.Run("apply upserts and deletes", func(t *testing.T) {
		existing := map[string]float32{"a": 1.0, "b": 2.0}
		set := map[string]float32{
			"a": 3.0, // update
			"c": 4.0, // insert
		}
		unset := []string{"b", "x"} // delete existing b and non-existent x (no-op)

		out, err := mergeFairnessWeightOverrides(existing, set, unset, 10)
		assert.NoError(t, err)
		assert.EqualValues(t, map[string]float32{"a": 3.0, "c": 4.0}, out)
	})

	t.Run("no update", func(t *testing.T) {
		// nil set and unset
		existing := map[string]float32{"a": 1.2, "b": 3.4}
		out, err := mergeFairnessWeightOverrides(existing, nil, nil, 10)
		assert.NoError(t, err)
		assert.EqualValues(t, existing, out)

		// empty set and unset
		existing = map[string]float32{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, map[string]float32{}, []string{}, 10)
		assert.NoError(t, err)
		assert.EqualValues(t, existing, out)

		// non-existent key in unset
		existing = map[string]float32{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, map[string]float32{}, []string{"does-not-exist"}, 2)
		assert.NoError(t, err)
		assert.EqualValues(t, existing, out)

		// same key and value
		existing = map[string]float32{"a": 1.2, "b": 3.4}
		out, err = mergeFairnessWeightOverrides(existing, map[string]float32{"a": 1.2}, []string{}, 2)
		assert.NoError(t, err)
		assert.EqualValues(t, existing, out)
	})

	t.Run("enforce capacity", func(t *testing.T) {
		existing := map[string]float32{"e": 1.0, "b": 2.0}
		unset := []string{"b"}                        // remove one first -> size becomes 1
		set := map[string]float32{"a": 3.0, "c": 4.0} // adding two makes size 3 which exceeds capacity 2
		out, err := mergeFairnessWeightOverrides(existing, set, unset, 2)
		assert.ErrorIs(t, err, errFairnessOverridesUpdateRejected)
		assert.Nil(t, out)
	})

	t.Run("check capacity after deletes", func(t *testing.T) {
		existing := map[string]float32{"a": 1.0, "b": 2.0}
		unset := []string{"b"}              // first, delete one
		set := map[string]float32{"c": 5.0} // then, add one
		out, err := mergeFairnessWeightOverrides(existing, set, unset, 2)
		assert.NoError(t, err)
		assert.Equal(t, map[string]float32{"a": 1.0, "c": 5.0}, out)
	})
}
