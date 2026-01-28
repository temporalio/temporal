package tests

import (
	"os"
	"testing"

	"go.temporal.io/server/tests/testcore"
)

func TestMain(m *testing.M) {
	code := m.Run()

	// Print cluster stats at the end of the test run (useful for CI)
	if os.Getenv("CI") != "" {
		testcore.PrintClusterStats()
	}

	os.Exit(code)
}
