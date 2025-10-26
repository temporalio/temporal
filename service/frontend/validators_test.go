package frontend

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateFairnessWeightUpdate(t *testing.T) {
	t.Run("set overrides", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
			"b": 2.3,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.NoError(t, err)
	})

	t.Run("unset overrides", func(t *testing.T) {
		set := map[string]float32{}
		unset := []string{"z"}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.NoError(t, err)
	})

	t.Run("enforce max number of overrides", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
		}
		unset := []string{"z"}

		err := validateFairnessWeightUpdate(set, unset, 10)
		require.NoError(t, err)

		err = validateFairnessWeightUpdate(set, unset, 2)
		require.NoError(t, err)

		err = validateFairnessWeightUpdate(set, unset, 1)
		require.ErrorContains(t, err, "too many fairness weight overrides in request: got 2, maximum 1")
	})

	t.Run("reject too long key in `set`", func(t *testing.T) {
		set := map[string]float32{
			strings.Repeat("abcdefg", 10): 1.0,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.ErrorContains(t, err, "fairness key length exceeds limit")
	})

	t.Run("reject too long key in `unset`", func(t *testing.T) {
		set := map[string]float32{"a": 1.0}
		unset := []string{strings.Repeat("abcdefg", 10)}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.ErrorContains(t, err, "fairness key length exceeds limit")
	})

	t.Run("reject negative weight", func(t *testing.T) {
		set := map[string]float32{
			"a": -2.0,
		}
		unset := []string{}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.ErrorContains(t, err, "invalid fairness weight weight for key \"a\": must be greater than zero")
	})

	t.Run("reject overlap between `set` and `unset`", func(t *testing.T) {
		set := map[string]float32{
			"a": 1.0,
		}
		unset := []string{"a"}
		err := validateFairnessWeightUpdate(set, unset, 10)
		require.ErrorContains(t, err, "fairness weight override key \"a\" present in both set and unset lists")
	})
}
