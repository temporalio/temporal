package frontend

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateFairnessWeightUpdate(t *testing.T) {
	t.Run("set overrides", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
			"b": 2.3,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.NoError(t, err)
	})

	t.Run("unset overrides", func(t *testing.T) {
		set := map[string]float32{}
		unset := []string{"z"}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.NoError(t, err)
	})

	t.Run("enforce max number of overrides", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
		}
		unset := []string{"z"}

		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.NoError(t, err)

		err = validateFairnessWeightUpdate(set, unset, 2)
		assert.NoError(t, err)

		err = validateFairnessWeightUpdate(set, unset, 1)
		assert.ErrorContains(t, err, "too many fairness weight overrides in request: got 2, maximum 1")
	})

	t.Run("reject empty key in `set`", func(t *testing.T) {
		set := map[string]float32{
			"   ": 1.0,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.ErrorContains(t, err, "fairness weight override key must not be empty")
	})

	t.Run("reject empty key in `unset`", func(t *testing.T) {
		set := map[string]float32{"a": 1.0}
		unset := []string{"   "}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.ErrorContains(t, err, "fairness weight override key must not be empty")
	})

	t.Run("reject negative weight", func(t *testing.T) {
		set := map[string]float32{
			"a": -2.0,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.ErrorContains(t, err, "invalid fairness weight weight for key \"a\": must be greater than zero")
	})

	t.Run("reject overlap between `set` and `unset`", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
		}
		unset := []string{"a"}
		err := validateFairnessWeightUpdate(set, unset, 10)
		assert.ErrorContains(t, err, "fairness weight override key \"a\" present in both set and unset lists")
	})
}
