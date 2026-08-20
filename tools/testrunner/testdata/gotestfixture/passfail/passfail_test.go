package passfail

import (
	"os"
	"testing"
)

func TestPass(t *testing.T) {}

func TestFail(t *testing.T) {
	if os.Getenv("TEMPORAL_TESTRUNNER_FIXTURE_FAILURE") == "" {
		return
	}
	t.Errorf("fixture failure")
}

func TestCleanupFailure(t *testing.T) {
	if os.Getenv("TEMPORAL_TESTRUNNER_FIXTURE_CLEANUP_FAILURE") == "" {
		return
	}
	t.Run("Child", func(t *testing.T) {
		t.Cleanup(func() {
			t.Errorf("fixture cleanup failure")
		})
	})
}
