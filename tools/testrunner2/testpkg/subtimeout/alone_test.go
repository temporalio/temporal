package subtimeout

import (
	"os"
	"testing"
	"time"
)

func TestAlone(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		time.Sleep(time.Minute) //nolint:forbidigo // intentional sleep to test timeout handling
	}
}
