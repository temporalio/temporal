package flaky

import (
	"os"
	"testing"
)

func TestStable(t *testing.T) { t.Log("stable") }

// TestFlaky is a top-level test that fails on the first attempt.
func TestFlaky(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		t.Fatal("intentional first-attempt failure")
	}
}

// TestSuite has subtests where FailChild fails on the first attempt.
// PassChild runs first and passes, so the retry should skip it via -test.skip.
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
