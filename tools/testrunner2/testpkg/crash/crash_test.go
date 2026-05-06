package crash

import (
	"os"
	"testing"
)

func TestCrash(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		// Panic in a non-test goroutine to crash the process.
		// A panic in the test goroutine would be recovered by the test
		// framework and only produce a test failure, not a crash.
		go panic("intentional crash")
		select {} // block until the process crashes
	}
}
