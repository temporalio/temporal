package packagefail

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	if os.Getenv("TEMPORAL_TEST_ATTEMPT") == "1" && code == 0 {
		os.Exit(1) //nolint:revive // intentionally exercise a package-level process failure.
	}
	os.Exit(code) //nolint:revive // preserve TestMain exit behavior for the fixture.
}

func TestPackageLevelFailure(t *testing.T) {
	t.Log("test passed before package-level failure")
}
