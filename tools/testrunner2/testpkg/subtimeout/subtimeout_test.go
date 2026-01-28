package subtimeout

import (
	"os"
	"testing"
	"time"
)

// TestWithSub has a subtest that will timeout.
// Used to verify TIMEOUT alerts show "TestWithSub/Child" (the leaf subtest)
// and NOT "TestWithSub" (the parent) separately.
func TestWithSub(t *testing.T) {
	t.Run("Child", func(t *testing.T) {
		if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
			time.Sleep(time.Minute)
		}
	})
}
