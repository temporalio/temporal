package timeout

import (
	"os"
	"testing"
	"time"
)

func TestQuick(t *testing.T) { t.Log("quick") }

func TestSlowOnce(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") != "1" {
		return // pass on retry
	}
	time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to test timeout handling
}
