package testrunner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAttemptResultFailedLeafTestsArePackageAndOccurrenceAware(t *testing.T) {
	result := attemptResult{packages: []packageResult{
		{
			name:    "example.com/one",
			outcome: packageFailed,
			executions: []testExecution{
				{id: testID{"example.com/one", "TestSuite"}, occurrence: 0, outcome: testFailed},
				{id: testID{"example.com/one", "TestSuite/TestChild"}, occurrence: 0, outcome: testFailed},
				{id: testID{"example.com/one", "TestSuite"}, occurrence: 1, outcome: testFailed},
			},
		},
		{
			name:    "example.com/two",
			outcome: packageFailed,
			executions: []testExecution{
				{id: testID{"example.com/two", "TestSuite/TestChild"}, outcome: testFailed},
			},
		},
		{
			name:    "example.com/structural",
			outcome: packageFailed,
			executions: []testExecution{
				{id: testID{"example.com/structural", "TestSuite"}, outcome: testFailed},
				{id: testID{"example.com/structural", "TestSuite/TestChild"}, outcome: testPassed},
				{
					id:      testID{"example.com/structural", "TestCleanupSuite"},
					outcome: testFailed,
					failure: failureEvidence{details: "cleanup failed", actionable: true},
				},
				{id: testID{"example.com/structural", "TestCleanupSuite/TestChild"}, outcome: testPassed},
			},
		},
	}}

	require.Equal(t, []testID{
		{"example.com/one", "TestSuite"},
		{"example.com/one", "TestSuite/TestChild"},
		{"example.com/structural", "TestCleanupSuite"},
		{"example.com/structural", "TestSuite"},
		{"example.com/two", "TestSuite/TestChild"},
	}, result.failedLeafTests())
}

func TestPackageResultMeaningfulIncompleteExecutions(t *testing.T) {
	pkg := packageResult{
		name:    "example.com/tests",
		outcome: packageFailed,
		executions: []testExecution{
			{id: testID{"example.com/tests", "TestStructural"}, outcome: testIncomplete},
			{id: testID{"example.com/tests", "TestStructural/Child"}, outcome: testPassed},
			{id: testID{"example.com/tests", "TestAborted"}, outcome: testIncomplete},
		},
	}

	require.Equal(t, []testExecution{{
		id:      testID{"example.com/tests", "TestAborted"},
		outcome: testIncomplete,
	}}, pkg.meaningfulIncompleteExecutions())

	pkg.outcome = packagePassed
	require.Empty(t, pkg.meaningfulIncompleteExecutions())
}

func TestAttemptResultTargetsActionableIncompleteFailure(t *testing.T) {
	id := testID{"example.com/tests", "TestSuite/TestCleanupFailure"}
	result := attemptResult{
		packages: []packageResult{{
			name:    id.packageName,
			outcome: packageFailed,
			executions: []testExecution{{
				id:      id,
				outcome: testIncomplete,
				failure: failureEvidence{details: "test exceeded timeout", actionable: true},
			}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	require.Equal(t, []testID{id}, result.failedLeafTests())
	require.Empty(t, result.abortedPackages())
	require.True(t, result.canTargetFailures())
}

func TestPackageResultMatchesIncompleteAncestorsByOccurrence(t *testing.T) {
	pkg := packageResult{
		name:    "example.com/tests",
		outcome: packageFailed,
		executions: []testExecution{
			{id: testID{"example.com/tests", "TestRepeated"}, occurrence: 0, outcome: testIncomplete},
			{id: testID{"example.com/tests", "TestRepeated/Child"}, occurrence: 1, outcome: testPassed},
		},
	}

	require.Equal(t, []testExecution{pkg.executions[0]}, pkg.meaningfulIncompleteExecutions())
}

func TestAttemptResultClassifiesBuildRuntimeAndProcessFailures(t *testing.T) {
	runtimePackage := packageResult{name: "example.com/runtime", outcome: packageFailed, output: "TestMain failed"}
	buildPackage := packageResult{name: "example.com/build", outcome: packageFailed, failedBuild: "example.com/build.test"}
	result := attemptResult{
		packages: []packageResult{runtimePackage, buildPackage},
		builds:   []buildResult{{importPath: "example.com/build.test", failed: true, output: "compile failed"}},
		process:  processResult{state: processExited, exitCode: 1},
	}

	require.Equal(t, []packageResult{runtimePackage}, result.runtimeFailures())
	require.False(t, result.unexplainedProcessFailure())
	require.False(t, result.canTargetFailures())
	require.False(t, result.successful())

	result = attemptResult{
		unstructuredOutput: "go: invalid invocation",
		process:            processResult{state: processExited, exitCode: 2},
	}
	require.True(t, result.unexplainedProcessFailure())
}

func TestAttemptResultObservedIncludesIncompleteExecution(t *testing.T) {
	id := testID{"example.com/tests", "TestObserved"}
	result := attemptResult{packages: []packageResult{{
		name:       id.packageName,
		executions: []testExecution{{id: id, outcome: testIncomplete, duration: time.Second}},
	}}}

	require.True(t, result.observed(id))
	require.False(t, result.observed(testID{id.packageName, "TestMissing"}))
}

func TestAttemptResultDoesNotAbortSuccessfulTruncatedStream(t *testing.T) {
	result := attemptResult{
		packages: []packageResult{{
			name:       "example.com/tests",
			outcome:    packageIncomplete,
			executions: []testExecution{{id: testID{"example.com/tests", "TestTruncated"}, outcome: testIncomplete}},
		}},
		process: processResult{state: processExited, exitCode: 0},
	}

	require.Empty(t, result.abortedPackages())
	require.True(t, result.successful())
}

func TestAttemptResultDiagnosticAndProcessStateRules(t *testing.T) {
	t.Run("package timeout suppresses incomplete package failures", func(t *testing.T) {
		packageName := "example.com/tests"
		result := attemptResult{
			packages: []packageResult{{
				name:       packageName,
				outcome:    packageFailed,
				executions: []testExecution{{id: testID{packageName, "TestIncomplete"}, outcome: testIncomplete}},
			}},
			diagnostics: []diagnostic{{kind: diagnosticTimeout, packageName: packageName}},
			process:     processResult{state: processExited, exitCode: 1},
		}

		require.Empty(t, result.abortedPackages())
		require.Empty(t, result.runtimeFailures())
	})

	t.Run("unbound diagnostic prevents targeted retry", func(t *testing.T) {
		failed := testID{"example.com/tests", "TestFailed"}
		result := attemptResult{
			packages: []packageResult{{
				name:       failed.packageName,
				outcome:    packageFailed,
				executions: []testExecution{{id: failed, outcome: testFailed}},
			}},
			diagnostics: []diagnostic{{kind: diagnosticDataRace}},
			process:     processResult{state: processExited, exitCode: 1},
		}

		require.False(t, result.canTargetFailures())
	})

	t.Run("timeout diagnostic prevents targeted retry", func(t *testing.T) {
		failed := testID{"example.com/tests", "TestFailed"}
		result := attemptResult{
			packages: []packageResult{{
				name:       failed.packageName,
				outcome:    packageFailed,
				executions: []testExecution{{id: failed, outcome: testFailed}},
			}},
			diagnostics: []diagnostic{{kind: diagnosticTimeout, packageName: failed.packageName, tests: []testID{failed}}},
			process:     processResult{state: processExited, exitCode: 1},
		}

		require.False(t, result.canTargetFailures())
	})

	t.Run("signaled process without failure evidence is unexplained", func(t *testing.T) {
		result := attemptResult{process: processResult{state: processSignaled}}
		require.True(t, result.unexplainedProcessFailure())
	})
}
