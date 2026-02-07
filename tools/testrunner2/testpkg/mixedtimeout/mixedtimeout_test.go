package mixedtimeout

import (
	"os"
	"testing"
	"time"
)

// TestMixed has mixed-depth subtests. On attempt 1, Param/Var2 times out.
// This exercises the quarantine logic: SimpleA and SimpleB pass at depth 2,
// Param/Var1 passes at depth 3, and Param/Var2 hangs (Var3 is never reached).
// On retry, Param is quarantined (run separately skipping Var1) while the
// regular retry skips SimpleA, SimpleB, and Param entirely.
// Var3 verifies that siblings of the stuck test run correctly in the quarantine.
func TestMixed(t *testing.T) {
	t.Run("SimpleA", func(t *testing.T) { t.Log("pass") })
	t.Run("SimpleB", func(t *testing.T) { t.Log("pass") })
	t.Run("Param", func(t *testing.T) {
		t.Run("Var1", func(t *testing.T) { t.Log("pass") })
		t.Run("Var2", func(t *testing.T) {
			if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
				time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to test timeout handling
			}
		})
		t.Run("Var3", func(t *testing.T) { t.Log("pass") }) // never reached on attempt 1
	})
	t.Run("SimpleC", func(t *testing.T) { t.Log("pass") }) // never reached on attempt 1
}
