package crashonce

import (
	"os"
	"testing"
)

func TestCrashOnce(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		go func() {
			var p *int
			_ = *p
		}() // crash
		select {}
	}
}
