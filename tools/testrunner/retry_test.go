package testrunner

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRetryPolicyPlansTargetedPackageAwareRetry(t *testing.T) {
	result := attemptResult{
		packages: []packageResult{
			{name: "example.com/one", outcome: packageFailed, executions: []testExecution{{id: testID{"example.com/one", "TestSuite/TestA"}, outcome: testFailed}}},
			{name: "example.com/two", outcome: packageFailed, executions: []testExecution{{id: testID{"example.com/two", "TestSuite/TestA"}, outcome: testFailed}}},
		},
		process: processResult{state: processExited, exitCode: 1},
	}
	plan := (retryPolicy{targetedThreshold: 20}).plan(result)

	require.Equal(t, retryTargeted, plan.mode)
	require.Equal(t, []testID{
		{"example.com/one", "TestSuite/TestA"},
		{"example.com/two", "TestSuite/TestA"},
	}, plan.expected)
	require.Equal(t, []string{
		"./...", "-run", "^TestSuite$/^TestA$", "-args", "-persistenceType=sql",
	}, plan.apply([]string{"./...", "-run=old", "-args", "-persistenceType=sql"}))
}

func TestRetryPlanPreservesTestBinaryRunFlag(t *testing.T) {
	plan := retryPlan{
		mode:     retryTargeted,
		expected: []testID{{"example.com/tests", "TestFailed"}},
	}

	require.Equal(t, []string{
		"./...", "-run", "^TestFailed$", "-args", "-run", "binary-pattern",
	}, plan.apply([]string{"./...", "-run=old", "-args", "-run", "binary-pattern"}))
}

func TestRetryPolicyRepeatsScopeForUnsafeFailuresAndThreshold(t *testing.T) {
	aborted := attemptResult{
		packages: []packageResult{{
			name:       "example.com/tests",
			outcome:    packageFailed,
			executions: []testExecution{{id: testID{"example.com/tests", "TestIncomplete"}, outcome: testIncomplete}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}
	plan := (retryPolicy{targetedThreshold: 20}).plan(aborted)
	require.Equal(t, retryRepeatScope, plan.mode)
	require.Equal(t, []string{"./...", "-run=old"}, plan.apply([]string{"./...", "-run=old"}))

	var executions []testExecution
	for i := range 3 {
		executions = append(executions, testExecution{
			id:      testID{"example.com/tests", "Test" + string(rune('A'+i))},
			outcome: testFailed,
		})
	}
	threshold := attemptResult{
		packages: []packageResult{{name: "example.com/tests", outcome: packageFailed, executions: executions}},
		process:  processResult{state: processExited, exitCode: 1},
	}
	require.Equal(t, retryRepeatScope, (retryPolicy{targetedThreshold: 2}).plan(threshold).mode)
}

func TestRetryPolicyStopsForSuccessAndTimeout(t *testing.T) {
	success := attemptResult{process: processResult{state: processExited}}
	require.Equal(t, retryStop, (retryPolicy{targetedThreshold: 20}).plan(success).mode)

	timedOut := attemptResult{
		diagnostics: []diagnostic{{kind: diagnosticTimeout}},
		process:     processResult{state: processExited, exitCode: 1},
	}
	require.Equal(t, retryStop, (retryPolicy{targetedThreshold: 20}).plan(timedOut).mode)
}

func TestRetryPlanValidatesEveryPackageAwareTarget(t *testing.T) {
	one := testID{"example.com/one", "TestSame"}
	two := testID{"example.com/two", "TestSame"}
	plan := retryPlan{mode: retryTargeted, expected: []testID{one, two}}
	next := attemptResult{
		packages: []packageResult{
			{name: one.packageName, executions: []testExecution{{id: one, outcome: testPassed}}},
			{name: two.packageName, outcome: packageFailed, failedBuild: "example.com/two.test"},
		},
		builds:  []buildResult{{importPath: "example.com/two.test", failed: true}},
		process: processResult{state: processExited, exitCode: 1},
	}
	require.NoError(t, plan.validate(next))

	next.packages[1].failedBuild = ""
	next.packages[1].outcome = packagePassed
	next.builds = nil
	require.EqualError(t, plan.validate(next), "expected targeted rerun was not observed: example.com/two.TestSame")
}

func TestRetryPlanDoesNotLetAnotherPackageBlockObservation(t *testing.T) {
	expected := testID{"example.com/expected", "TestMissing"}
	plan := retryPlan{mode: retryTargeted, expected: []testID{expected}}
	next := attemptResult{
		packages: []packageResult{{
			name:        "example.com/other",
			outcome:     packageFailed,
			failedBuild: "example.com/other.test",
		}},
		builds:  []buildResult{{importPath: "example.com/other.test", failed: true}},
		process: processResult{state: processExited, exitCode: 1},
	}

	require.EqualError(t, plan.validate(next), "expected targeted rerun was not observed: example.com/expected.TestMissing")
}

func TestRetryPolicyHandlesNonTargetableFailures(t *testing.T) {
	tests := []struct {
		name string
		give attemptResult
		want retryMode
	}{
		{
			name: "build",
			give: attemptResult{
				builds:  []buildResult{{importPath: "example.com/tests.test", failed: true}},
				process: processResult{state: processExited, exitCode: 1},
			},
			want: retryRepeatScope,
		},
		{
			name: "runtime",
			give: attemptResult{
				packages: []packageResult{{name: "example.com/tests", outcome: packageFailed}},
				process:  processResult{state: processExited, exitCode: 1},
			},
			want: retryRepeatScope,
		},
		{
			name: "unexplained process",
			give: attemptResult{process: processResult{state: processExited, exitCode: 1}},
			want: retryStop,
		},
		{
			name: "deadline",
			give: attemptResult{process: processResult{state: processDeadlineExceeded, exitCode: 1}},
			want: retryStop,
		},
		{
			name: "start failure",
			give: attemptResult{process: processResult{state: processStartFailed, exitCode: 1}},
			want: retryStop,
		},
		{
			name: "signaled",
			give: attemptResult{process: processResult{state: processSignaled, exitCode: 1}},
			want: retryStop,
		},
		{
			name: "wait failure",
			give: attemptResult{process: processResult{state: processWaitFailed, exitCode: 1}},
			want: retryStop,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, (retryPolicy{targetedThreshold: 20}).plan(test.give).mode)
		})
	}
}

func TestRetryPolicyRepeatsScopeWhenDiagnosticAlsoNamesPassingTest(t *testing.T) {
	failed := testID{"example.com/tests", "TestFailed"}
	passed := testID{"example.com/tests", "TestPassed"}
	result := attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{
				{id: failed, outcome: testFailed},
				{id: passed, outcome: testPassed},
			},
		}},
		diagnostics: []diagnostic{{
			kind:  diagnosticDataRace,
			tests: []testID{failed, passed},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	require.Equal(t, retryRepeatScope, (retryPolicy{targetedThreshold: 20}).plan(result).mode)
}
