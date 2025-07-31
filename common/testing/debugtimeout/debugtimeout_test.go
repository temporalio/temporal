//go:build test_dep

package debugtimeout

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDebugTimeoutMultiplierInTestFiles(t *testing.T) {

	t.Run("DefaultValueInTestFiles", func(t *testing.T) {
		assert.Equal(t, time.Duration(1), Multiplier, "Multiplier should be 1 by default in test files")
	})

	t.Run("EnvironmentVariableOverride", func(t *testing.T) {
		// Save original environment variable value
		originalValue := os.Getenv("TEMPORAL_DEBUG_TIMEOUT")

		// Restore original environment variable after test
		defer func() {
			if originalValue == "" {
				os.Unsetenv("TEMPORAL_DEBUG_TIMEOUT")
			} else {
				os.Setenv("TEMPORAL_DEBUG_TIMEOUT", originalValue)
			}
		}()

		// Check that multiplier is set to default.
		err := os.Setenv("TEMPORAL_DEBUG_TIMEOUT", "")
		require.NoError(t, err)

		assert.Equal(t, time.Duration(10), getMultiplier(),
			"Multiplier should be 10 when TEMPORAL_DEBUG_TIMEOUT is set with an empty value")

		// Check that it uses the value from the environment variable.
		err = os.Setenv("TEMPORAL_DEBUG_TIMEOUT", "5")
		require.NoError(t, err)

		assert.Equal(t, time.Duration(5), getMultiplier(),
			"Multiplier should be overridden by TEMPORAL_DEBUG_TIMEOUT environment variable")

		// Check that it panics for invalid values.
		err = os.Setenv("TEMPORAL_DEBUG_TIMEOUT", "abc")
		require.NoError(t, err)

		require.Panics(t, func() {
			_ = getMultiplier()
		}, "Expected panic for non-numeric TEMPORAL_DEBUG_TIMEOUT")
	})
}
