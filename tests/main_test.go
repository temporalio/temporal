package tests

import (
	"os"
	"testing"

	"go.temporal.io/server/tests/testcore"
)

func TestMain(m *testing.M) {
	code := m.Run()

	// Write cluster stats to file for CI aggregation
	if os.Getenv("CI") != "" {
		testcore.WriteClusterStats()
	}

	os.Exit(code)
}
