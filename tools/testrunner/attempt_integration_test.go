package testrunner

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunAttemptConsumesRealGoTestJSON(t *testing.T) {
	t.Setenv("TEMPORAL_TESTRUNNER_FIXTURE_FAILURE", "1")
	result := runAttempt(context.Background(), attemptSpec{
		number: 1,
		args: []string{
			"./testdata/gotestfixture/passfail",
			"-run", "TestPass|TestFail",
		},
	})
	require.Equal(t, 1, result.process.exitCode)
	require.Len(t, result.packages, 1)
	require.Equal(t, packageFailed, result.packages[0].outcome)
	require.Equal(t, []testID{{
		packageName: "go.temporal.io/server/tools/testrunner/testdata/gotestfixture/passfail",
		testName:    "TestFail",
	}}, result.failedLeafTests())
}

func TestRunAttemptConsumesRealGoBuildFailureJSON(t *testing.T) {
	result := runAttempt(context.Background(), attemptSpec{
		number: 1,
		args:   []string{"./testdata/gotestfixture/buildfail"},
	})
	require.Equal(t, 1, result.process.exitCode)
	require.NotEmpty(t, result.builds)
	require.True(t, result.builds[0].failed)
	require.Contains(t, result.builds[0].output, "undefinedFixtureSymbol")
	require.False(t, result.canTargetFailures())
}

func TestRunAttemptAttributesCleanupFailureFromRealGoTestJSON(t *testing.T) {
	t.Setenv("TEMPORAL_TESTRUNNER_FIXTURE_CLEANUP_FAILURE", "1")
	result := runAttempt(context.Background(), attemptSpec{
		number: 1,
		args: []string{
			"./testdata/gotestfixture/passfail",
			"-run", "TestCleanupFailure",
		},
	})
	require.Equal(t, []testID{{
		packageName: "go.temporal.io/server/tools/testrunner/testdata/gotestfixture/passfail",
		testName:    "TestCleanupFailure/Child",
	}}, result.failedLeafTests())
	require.Empty(t, result.abortedPackages())
	require.True(t, result.canTargetFailures())
}
