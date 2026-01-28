package subtimeout

import (
	"os"
	"testing"
	"time"
)

func TestAlone(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		time.Sleep(time.Minute)
	}
}
