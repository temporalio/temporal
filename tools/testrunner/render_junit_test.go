package testrunner

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/junit"
)

func TestRenderJUnitSuppressesResolvedNoDetailParents(t *testing.T) {
	id := func(name string) testID { return testID{"example.com/tests", name} }
	first := attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{
				{id: id("TestEmptyParent"), outcome: testFailed},
				{id: id("TestEmptyParent/Child"), outcome: testPassed},
				{id: id("TestNoisyParent"), outcome: testFailed, output: "network dial warning\n"},
				{id: id("TestNoisyParent/Child"), outcome: testPassed},
				{id: id("TestCleanupParent"), outcome: testFailed, failure: failureEvidence{details: "Error Trace:\tparent_test.go:20\nError:\tcleanup failed", actionable: true}},
				{id: id("TestCleanupParent/Child"), outcome: testPassed},
			},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}
	final := attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{
				{id: id("TestEmptyParent"), outcome: testPassed},
				{id: id("TestEmptyParent/Child"), outcome: testPassed},
				{id: id("TestNoisyParent"), outcome: testPassed},
				{id: id("TestNoisyParent/Child"), outcome: testPassed},
				{id: id("TestCleanupParent"), outcome: testPassed},
				{id: id("TestCleanupParent/Child"), outcome: testPassed},
				{id: id("TestFinalParent"), outcome: testFailed},
				{id: id("TestFinalParent/Child"), outcome: testPassed},
			},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	report := renderJUnit([]attemptResult{first, final})
	require.NoError(t, junit.ValidateCounters(&report))
	names := collectJUnitTestNames(report.Suites)
	require.NotContains(t, names, "TestEmptyParent")
	require.NotContains(t, names, "TestNoisyParent")
	require.Contains(t, names, "TestCleanupParent")
	require.Contains(t, names, "TestFinalParent (retry 1) (final)")

	finalFailure := findJUnitTestcase(t, report, "TestFinalParent (retry 1) (final)")
	require.NotNil(t, finalFailure.Failure)
	require.NotEmpty(t, finalFailure.Failure.Data)
	summary := newSummaryFromReports(&report)
	require.Len(t, summary.Rows, 2)
	for _, row := range summary.Rows {
		require.NotContains(t, row.Name, "TestEmptyParent")
		require.NotContains(t, row.Name, "TestNoisyParent")
		require.NotEmpty(t, row.Details)
	}
}

func TestRenderJUnitSuppressesPropagatedParentUnlessActionable(t *testing.T) {
	pkg := packageResult{
		name:    "example.com/tests",
		outcome: packageFailed,
		executions: []testExecution{
			{id: testID{"example.com/tests", "TestGeneric"}, outcome: testFailed, output: "ordinary parent log\n"},
			{id: testID{"example.com/tests", "TestGeneric/Child"}, outcome: testFailed},
			{id: testID{"example.com/tests", "TestActionable"}, outcome: testFailed, failure: failureEvidence{details: "Error Trace:\tparent_test.go:20", actionable: true}},
			{id: testID{"example.com/tests", "TestActionable/Child"}, outcome: testFailed},
		},
	}
	report := renderJUnit([]attemptResult{{
		packages: []packageResult{pkg},
		process:  processResult{state: processExited, exitCode: 1},
	}})

	require.NoError(t, junit.ValidateCounters(&report))
	names := collectJUnitTestNames(report.Suites)
	require.NotContains(t, names, "TestGeneric")
	require.Contains(t, names, "TestGeneric/Child")
	require.Contains(t, names, "TestActionable")
	require.Contains(t, names, "TestActionable/Child")
}

func TestRenderJUnitRendersActionableIncompleteFailure(t *testing.T) {
	report := renderJUnit([]attemptResult{{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{
				{id: testID{"example.com/tests", "TestSuite"}, outcome: testFailed},
				{
					id:      testID{"example.com/tests", "TestSuite/TestCleanupFailure"},
					outcome: testIncomplete,
					failure: failureEvidence{details: "test exceeded timeout", actionable: true},
				},
				{id: testID{"example.com/tests", "TestSuite/TestCleanupFailure/Completed"}, outcome: testPassed},
			},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}})

	require.NoError(t, junit.ValidateCounters(&report))
	require.Equal(t, []string{
		"TestSuite/TestCleanupFailure",
		"TestSuite/TestCleanupFailure/Completed",
	}, collectJUnitTestNames(report.Suites))
	testcase := findJUnitTestcase(t, report, "TestSuite/TestCleanupFailure")
	require.NotNil(t, testcase.Failure)
	require.Equal(t, "test exceeded timeout", testcase.Failure.Data)
}

func TestRenderJUnitBuildAbortDiagnosticAndCoverage(t *testing.T) {
	coverage := 12.5
	result := attemptResult{
		packages: []packageResult{{
			name:       "example.com/tests",
			outcome:    packageFailed,
			coverage:   &coverage,
			executions: []testExecution{{id: testID{"example.com/tests", "TestIncomplete"}, outcome: testIncomplete}},
		}},
		builds: []buildResult{{importPath: "example.com/broken.test", failed: true, output: "compile failed"}},
		diagnostics: []diagnostic{{
			kind: diagnosticPanic, summary: "boom", details: "panic: boom",
			tests: []testID{{"example.com/tests", "TestIncomplete"}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	report := renderJUnit([]attemptResult{result})
	require.NoError(t, junit.ValidateCounters(&report))
	require.Equal(t, 3, report.Tests)
	require.Equal(t, 2, report.Failures)
	require.Equal(t, 1, report.Errors)
	requireJUnitTimesAreNumeric(t, report)
}

func TestRenderJUnitAbortIncludesDiagnosticAndProcessContext(t *testing.T) {
	pkg := packageResult{
		name:       "example.com/tests",
		outcome:    packageIncomplete,
		executions: []testExecution{{id: testID{"example.com/tests", "TestIncomplete"}, outcome: testIncomplete}},
	}
	result := attemptResult{
		packages: []packageResult{pkg},
		diagnostics: []diagnostic{{
			kind:        diagnosticPanic,
			summary:     "boom",
			details:     "panic: boom",
			packageName: pkg.name,
		}},
		process: processResult{
			state:    processSignaled,
			exitCode: 1,
			details:  "killed",
			stderr:   "process stderr",
		},
	}

	report := renderJUnit([]attemptResult{result})
	testcase := findJUnitTestcase(t, report, "testrunner.PackageAborted: example.com/tests")
	require.NotNil(t, testcase.Failure)
	require.Contains(t, testcase.Failure.Data, "panic: boom")
	require.Contains(t, testcase.Failure.Data, "killed")
	require.Contains(t, testcase.Failure.Data, "process stderr")

	result.diagnostics = nil
	result.process = processResult{state: processExited, exitCode: 1, details: "exit status 1"}
	report = renderJUnit([]attemptResult{result})
	testcase = findJUnitTestcase(t, report, "testrunner.PackageAborted: example.com/tests")
	require.NotNil(t, testcase.Failure)
	require.Contains(t, testcase.Failure.Data, "exit status 1")
}

func TestRenderJUnitTotalTimeoutIncludesIncompleteTests(t *testing.T) {
	report := renderJUnit([]attemptResult{{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageIncomplete,
			executions: []testExecution{
				{id: testID{"example.com/tests", "TestHung"}, outcome: testIncomplete},
				{id: testID{"example.com/tests", "TestAlsoHung"}, outcome: testIncomplete},
			},
		}},
		process: processResult{state: processDeadlineExceeded, exitCode: 1, details: "context deadline exceeded"},
	}})

	testcase := findJUnitTestcase(t, report, "testrunner.TotalTimeout")
	require.NotNil(t, testcase.Failure)
	require.Contains(t, testcase.Failure.Data, "context deadline exceeded")
	require.Contains(t, testcase.Failure.Data, "TestHung")
	require.Contains(t, testcase.Failure.Data, "TestAlsoHung")
	require.NotContains(t, collectJUnitTestNames(report.Suites), "testrunner.PackageAborted: example.com/tests")
}

func TestRenderJUnitBoundsFailurePayloads(t *testing.T) {
	details := strings.Repeat("x", junitAlertDetailsMaxBytes+100)
	tests := []struct {
		name     string
		result   attemptResult
		caseName string
	}{
		{
			name: "test failure",
			result: attemptResult{
				packages: []packageResult{{
					name:       "example.com/tests",
					outcome:    packageFailed,
					executions: []testExecution{{id: testID{"example.com/tests", "TestFailed"}, outcome: testFailed, failure: failureEvidence{details: details}}},
				}},
				process: processResult{state: processExited, exitCode: 1},
			},
			caseName: "TestFailed",
		},
		{
			name: "runtime failure",
			result: attemptResult{
				packages: []packageResult{{name: "example.com/runtime", outcome: packageFailed, output: details}},
				process:  processResult{state: processExited, exitCode: 1},
			},
			caseName: "example.com/runtime [runtime failure]",
		},
		{
			name:     "execution error",
			result:   attemptResult{process: processResult{state: processExited, exitCode: 2, details: details}},
			caseName: "testrunner.ExecutionError",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testcase := findJUnitTestcase(t, renderJUnit([]attemptResult{test.result}), test.caseName)
			if testcase.Failure != nil {
				require.LessOrEqual(t, len(testcase.Failure.Data), junitAlertDetailsMaxBytes)
				return
			}
			require.NotNil(t, testcase.Error)
			require.LessOrEqual(t, len(testcase.Error.Data), junitAlertDetailsMaxBytes)
		})
	}
}

func TestRenderJUnitTimeoutDoesNotAlsoReportPackageAbort(t *testing.T) {
	pkg := packageResult{
		name:       "example.com/tests",
		outcome:    packageFailed,
		executions: []testExecution{{id: testID{"example.com/tests", "TestTimedOut"}, outcome: testIncomplete}},
	}
	report := renderJUnit([]attemptResult{{
		packages: []packageResult{pkg},
		diagnostics: []diagnostic{{
			kind:        diagnosticTimeout,
			summary:     "panic: test timed out after 5s",
			details:     "abridged stacktrace",
			packageName: pkg.name,
			tests:       []testID{{pkg.name, "TestTimedOut"}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}})

	names := collectJUnitTestNames(report.Suites)
	require.Contains(t, names, "TestTimedOut (timed out)")
	require.NotContains(t, names, "testrunner.PackageAborted: example.com/tests")
}

func TestRenderJUnitIterationSuffixIsNotAParentRelationship(t *testing.T) {
	result := attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{
				{id: testID{"example.com/tests", "TestRepeated"}, outcome: testFailed},
				{id: testID{"example.com/tests", "TestRepeated#01"}, outcome: testFailed},
			},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	report := renderJUnit([]attemptResult{result})
	require.NoError(t, junit.ValidateCounters(&report))
	require.ElementsMatch(t, []string{"TestRepeated", "TestRepeated#01"}, collectJUnitTestNames(report.Suites))
}

func TestRenderJUnitPreservesSuccessfulBuildOutput(t *testing.T) {
	report := renderJUnit([]attemptResult{{
		builds:  []buildResult{{importPath: "example.com/generated", output: "generated warning\n"}},
		process: processResult{state: processExited},
	}})

	require.NoError(t, junit.ValidateCounters(&report))
	require.Len(t, report.Suites, 1)
	require.Equal(t, "example.com/generated", report.Suites[0].Name)
	require.Empty(t, report.Suites[0].Testcases)
	require.NotNil(t, report.Suites[0].SystemOut)
	require.Equal(t, "generated warning", report.Suites[0].SystemOut.Data)
}

func TestRenderJUnitIncludesStderrInRuntimeFailure(t *testing.T) {
	report := renderJUnit([]attemptResult{{
		packages: []packageResult{{name: "example.com/tests", outcome: packageFailed}},
		process:  processResult{state: processExited, exitCode: 1, stderr: "runtime stderr\n"},
	}})

	testcase := findJUnitTestcase(t, report, "example.com/tests [runtime failure]")
	require.NotNil(t, testcase.Error)
	require.Contains(t, testcase.Error.Data, "runtime stderr")
}

func TestRenderJUnitKeepsEveryDiagnosticTestAssociation(t *testing.T) {
	report := renderJUnit([]attemptResult{{
		diagnostics: []diagnostic{{
			kind:    diagnosticPanic,
			summary: "boom",
			details: "panic: boom",
			tests: []testID{
				{"example.com/one", "TestOne"},
				{"example.com/two", "TestTwo"},
			},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}})

	testcase := findJUnitTestcase(t, report, "PANIC: boom — in TestOne")
	require.NotNil(t, testcase.Failure)
	require.Contains(t, testcase.Failure.Data, "example.com/one.TestOne")
	require.Contains(t, testcase.Failure.Data, "example.com/two.TestTwo")
}

func TestRenderCrashJUnitUsesNumericTimes(t *testing.T) {
	report := renderCrashJUnit("unit-test")
	require.NoError(t, junit.ValidateCounters(&report))
	requireJUnitTimesAreNumeric(t, report)
}

func collectJUnitTestNames(suites []junit.Testsuite) []string {
	var names []string
	for _, suite := range suites {
		for _, testcase := range suite.Testcases {
			names = append(names, testcase.Name)
		}
	}
	return names
}

func findJUnitTestcase(t *testing.T, report junit.Testsuites, name string) junit.Testcase {
	t.Helper()
	for _, suite := range report.Suites {
		for _, testcase := range suite.Testcases {
			if testcase.Name == name {
				return testcase
			}
		}
	}
	require.FailNowf(t, "testcase not found", "no testcase named %q, have %v", name, collectJUnitTestNames(report.Suites))
	return junit.Testcase{}
}

func requireJUnitTimesAreNumeric(t *testing.T, report junit.Testsuites) {
	t.Helper()
	_, err := strconv.ParseFloat(report.Time, 64)
	require.NoError(t, err)
	for _, suite := range report.Suites {
		_, err := strconv.ParseFloat(suite.Time, 64)
		require.NoError(t, err)
		for _, testcase := range suite.Testcases {
			_, err := strconv.ParseFloat(testcase.Time, 64)
			require.NoError(t, err)
		}
	}
}
