package flaky

import (
	"os"
	"testing"
)

func TestStable(t *testing.T) { t.Log("stable") }

func TestFlaky(t *testing.T) {
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" {
		t.Fatal("intentional first-attempt failure")
	}
}
