package singletimeout

import (
	"os"
	"testing"
	"time"
)

// TestSuite is the only top-level test. It has subtests at depth 2.
// On attempt 1, Slow times out. Pass1 and Pass2 complete before the timeout.
// This exercises the quarantine logic where the quarantined parent is the only
// test in the work unit: the quarantine plan should run TestSuite skipping Pass1
// and Pass2, while the regular plan (which would skip TestSuite entirely) must
// be suppressed since it would match zero tests.
func TestSuite(t *testing.T) {
	t.Run("Pass1", func(t *testing.T) { t.Log("pass") })
	t.Run("Pass2", func(t *testing.T) { t.Log("pass") })
	t.Run("Slow", func(t *testing.T) {
		if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
			time.Sleep(time.Minute) // timeout on first attempt
		}
	})
}
