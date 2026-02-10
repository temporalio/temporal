package subfail

import (
	"os"
	"testing"
)

// TestSuite has subtests; FailChild fails on first attempt.
// Go marks TestSuite itself as FAIL when any child fails.
// The runner's filterParentFailures must strip TestSuite from the retry list
// so that buildTestFilterPattern produces a correct -test.run pattern.
func TestSuite(t *testing.T) {
	t.Run("PassChild", func(t *testing.T) {
		t.Log("always passes")
	})
	t.Run("FailChild", func(t *testing.T) {
		if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
			t.Fatal("intentional subtest failure")
		}
	})
}

// TestOK is an independent top-level test that always passes.
func TestOK(t *testing.T) { t.Log("ok") }
