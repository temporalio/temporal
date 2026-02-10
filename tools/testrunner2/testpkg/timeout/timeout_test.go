package timeout

import (
	"os"
	"testing"
	"time"
)

func TestQuick(t *testing.T) { t.Log("quick") }

// TestSlowOnce is a top-level test that gets stuck on attempt 1.
func TestSlowOnce(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") != "1" {
		return // pass on retry
	}
	time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to test timeout handling
}

// TestWithSub has subtests where Slow gets stuck on attempt 1.
// Pass1 and Pass2 complete before the stuck detection fires, so the
// retry should target only TestWithSub/Slow and skip Pass1 and Pass2.
func TestWithSub(t *testing.T) {
	t.Run("Pass1", func(t *testing.T) { t.Log("pass") })
	t.Run("Pass2", func(t *testing.T) { t.Log("pass") })
	t.Run("Slow", func(t *testing.T) {
		if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
			time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to test timeout handling
		}
	})
}

// TestParentStuck has subtests that all pass, but the parent hangs after
// they complete (simulating a stuck teardown). The stuck detector should
// report the parent, and the retry should skip passed children.
func TestParentStuck(t *testing.T) {
	t.Run("Child1", func(t *testing.T) { t.Log("pass") })
	t.Run("Child2", func(t *testing.T) { t.Log("pass") })
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to simulate stuck teardown
	}
}
