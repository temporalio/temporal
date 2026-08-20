package testrunner

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/junit"
)

func mustReadTestsuitesFixture(t *testing.T, path string) *junit.Testsuites {
	t.Helper()
	report, err := junit.Read(path)
	require.NoError(t, err)
	return report
}
