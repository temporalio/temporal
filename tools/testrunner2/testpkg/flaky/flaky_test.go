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

// TestDeepSuite has 3 levels of nesting. GroupA has two passing subtests and
// one failing. This exercises the skip pattern builder with 3 levels to ensure
// "|" alternation stays within a single "/" level (Go's splitRegexp splits
// -test.skip patterns by "/" before matching).
func TestDeepSuite(t *testing.T) {
	t.Run("GroupA", func(t *testing.T) {
		t.Run("Pass1", func(t *testing.T) { t.Log("pass") })
		t.Run("Pass2", func(t *testing.T) { t.Log("pass") })
		t.Run("Fail1", func(t *testing.T) {
			if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
				t.Fatal("intentional deep failure")
			}
		})
	})
}
