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

// TestDeepSuite has 3 levels of nesting with two groups. Each group has a
// passing and a failing subtest with the same names ("Pass" and "Fail").
// This exercises that each failure triggers its own retry with a correctly
// scoped skip pattern — the overlapping names ensure skip patterns are
// properly scoped per group and don't accidentally cross group boundaries.
func TestDeepSuite(t *testing.T) {
	t.Run("GroupA", func(t *testing.T) {
		t.Run("Pass", func(t *testing.T) { t.Log("pass") })
		t.Run("Fail", func(t *testing.T) {
			if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
				t.Fatal("intentional deep failure")
			}
		})
	})
	t.Run("GroupB", func(t *testing.T) {
		t.Run("Pass", func(t *testing.T) { t.Log("pass") })
		t.Run("Fail", func(t *testing.T) {
			if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
				t.Fatal("intentional deep failure")
			}
		})
	})
}
