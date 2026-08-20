package testrunner

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/tools/common/junit"
)

func TestRunnerSanitizeAndParseArgs(t *testing.T) {
	t.Run("Passthrough", func(t *testing.T) {
		r := newRunner()
		args, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		})
		require.NoError(t, err)
		require.Equal(t, []string{
			"-foo",
			"bar",
			// max-attempts has been stripped
			"-coverprofile=test.cover.out",
			"baz",
		}, args)
		require.Equal(t, "test.xml", r.junitOutputPath)
		require.Equal(t, 3, r.maxAttempts)
		require.Equal(t, "test.cover.out", r.coverProfilePath)
	})

	t.Run("TotalTimeout", func(t *testing.T) {
		r := newRunner()
		args, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"--total-timeout=39m",
			"--",
			"-timeout=35m",
			"-coverprofile=test.cover.out",
		})
		require.NoError(t, err)
		require.Equal(t, 39*time.Minute, r.totalTimeout)
		require.NotContains(t, args, "--total-timeout=39m")
		require.Contains(t, args, "-timeout=35m")
	})

	t.Run("TotalTimeoutNotSetWhenNoGoTestTimeout", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"--",
			"-coverprofile=test.cover.out",
		})
		require.NoError(t, err)
		require.Zero(t, r.totalTimeout)
	})

	t.Run("TotalTimeoutInvalid", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"--total-timeout=invalid",
			"--",
			"-coverprofile=test.cover.out",
		})
		require.ErrorContains(t, err, `invalid argument "--total-timeout="`)
	})

	t.Run("GoTestSumPathIsOptional", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"-foo",
			"bar",
			// missing:
			// "--max-attempts=0",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		})
		require.NoError(t, err)
	})

	t.Run("AttemptsInvalid1", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"-foo",
			"bar",
			"--max-attempts=0", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		})
		require.ErrorContains(t, err, `invalid argument "--max-attempts=": must be greater than zero`)
	})

	t.Run("AttemptsInvalid2", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"-foo",
			"bar",
			"--max-attempts=invalid", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		})
		require.ErrorContains(t, err, `invalid argument "--max-attempts=": strconv.Atoi: parsing "invalid"`)
	})

	t.Run("AttemptsNegative", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--max-attempts=-1",
			"--",
			"-coverprofile=test.cover.out",
		})
		require.ErrorContains(t, err, `invalid argument "--max-attempts=": must be greater than zero`)
	})

	t.Run("SeparatorOnlyEndsRunnerFlagsOnce", func(t *testing.T) {
		r := newRunner()
		args, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--max-attempts=2",
			"--",
			"-coverprofile=test.cover.out",
			"-args",
			"--",
			"--max-attempts=99",
		})
		require.NoError(t, err)
		require.Equal(t, []string{
			"-coverprofile=test.cover.out", "-args", "--", "--max-attempts=99",
		}, args)
		require.Equal(t, 2, r.maxAttempts)
	})

	t.Run("JunitfileMissing", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			// missing:
			// "--junitfile=test.xml"
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		})
		require.ErrorContains(t, err, `missing required argument "--junitfile="`)
	})

	t.Run("CoverprofileMissing", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			// missing:
			// "-coverprofile=test.cover.out",
			"baz",
		})
		require.ErrorContains(t, err, `missing required argument "-coverprofile="`)
	})

	t.Run("WriteSummaryRequiresJunitGlob", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(summaryCommand, nil)
		require.ErrorContains(t, err, `missing required argument "--junit-glob="`)
	})

	t.Run("ReportCrashRequiresJunitfile", func(t *testing.T) {
		r := newRunner()
		_, err := r.sanitizeAndParseArgs(crashReportCommand, []string{
			"--crashreportname=my-test",
		})
		require.ErrorContains(t, err, `missing required argument "--junitfile="`)
	})
}

func TestStripRunFromArgs(t *testing.T) {
	t.Run("OneArg", func(t *testing.T) {
		args := stripRunFromArgs([]string{"-foo", "bar", "-run=A"})
		require.Equal(t, []string{"-foo", "bar"}, args)
	})

	t.Run("TwoArgs", func(t *testing.T) {
		args := stripRunFromArgs([]string{"-foo", "bar", "-run", "A"})
		require.Equal(t, []string{"-foo", "bar"}, args)
	})
}

func TestAttemptCoveragePath(t *testing.T) {
	r := newRunner()
	r.coverProfilePath = filepath.Join("reports", "coverage.out")
	require.Equal(t, filepath.Join("reports", "coverage.out_0.cover.out"), r.attemptCoveragePath(1))
	require.Equal(t, filepath.Join("reports", "coverage.out_1.cover.out"), r.attemptCoveragePath(2))
}

func TestWriteCurrentReport(t *testing.T) {
	r := newRunner()
	r.junitOutputPath = filepath.Join(t.TempDir(), "junit.xml")

	// Simulate attempt 1 completing with failures.
	r.results = append(r.results, attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{{
				id:      testID{packageName: "example.com/tests", testName: "TestOne"},
				outcome: testFailed,
			}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	})

	require.NoError(t, r.writeCurrentReport())

	result, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	require.Equal(t, 1, result.Failures)
	require.Len(t, result.Suites, 1)

	// Simulate attempt 2 also completing. The intermediate write should now
	// contain failures from both attempts, so that if the process is killed
	// before attempt 3 the file on disk already has the full picture.
	r.results = append(r.results, attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{{
				id:      testID{packageName: "example.com/tests", testName: "TestOne"},
				outcome: testFailed,
			}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	})

	require.NoError(t, r.writeCurrentReport())

	result2, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	require.Equal(t, 2, result2.Failures) // 1 retained leaf from each attempt
	require.Len(t, result2.Suites, 2)
	require.NoError(t, junit.ValidateCounters(result2))
}

func TestWriteReportRejectsInvalidCounters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junit.xml")
	require.NoError(t, os.WriteFile(path, []byte("previous report"), 0o644))
	r := newRunner()
	r.junitOutputPath = path
	report := junit.Testsuites{
		Tests: 2,
		Suites: []junit.Testsuite{{
			Name:      "suite",
			Tests:     1,
			Testcases: []junit.Testcase{{Name: "TestOne"}},
		}},
	}

	require.ErrorContains(t, r.writeReport(&report), "invalid JUnit report: root tests counter is 2, want 1")
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "previous report", string(content))
}

func TestRunnerReportCrash(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "junit-report.xml")

	r := newRunner()
	_, err := r.sanitizeAndParseArgs(crashReportCommand, []string{
		"--junitfile=" + out,
		"--crashreportname=my-test",
	})
	require.NoError(t, err)

	require.NoError(t, r.reportCrash())
	report, err := junit.Read(out)
	require.NoError(t, err)
	require.Len(t, report.Suites, 1)
	require.Len(t, report.Suites[0].Testcases, 1)
	require.Equal(t, "my-test (crash)", report.Suites[0].Testcases[0].Name)
	require.NotNil(t, report.Suites[0].Testcases[0].Failure)
	require.Equal(t, string(failureTypeCrash), report.Suites[0].Testcases[0].Failure.Type)
	require.Equal(t, 1, report.Failures)
}

func TestRunnerReportCrashReturnsWriteError(t *testing.T) {
	r := newRunner()
	r.crashName = "my-test"
	r.junitOutputPath = filepath.Join(t.TempDir(), "missing", "junit.xml")
	require.ErrorContains(t, r.reportCrash(), "failed to create temporary JUnit report file")
}

func TestRunnerPrintSummary(t *testing.T) {
	dir := t.TempDir()
	report1 := mustReadTestsuitesFixture(t, "testdata/junit-single-failure.xml")
	report1.Suites[0].Name = "SuiteA"
	report1.Suites[0].Testcases[0].Name = "TestAlpha"
	report1.Suites[0].Testcases[0].Failure.Type = string(failureTypeFailed)
	report1.Suites[0].Testcases[0].Failure.Data = "alpha failure"
	require.NoError(t, junit.Write(filepath.Join(dir, "junit.alpha.xml"), report1))
	report2 := mustReadTestsuitesFixture(t, "testdata/junit-single-failure.xml")
	report2.Suites[0].Name = "SuiteB"
	report2.Suites[0].Testcases[0].Name = "TestBeta"
	report2.Suites[0].Testcases[0].Failure.Type = string(failureTypeFailed)
	report2.Suites[0].Testcases[0].Failure.Data = "beta failure"
	require.NoError(t, junit.Write(filepath.Join(dir, "junit.beta.xml"), report2))

	r := newRunner()
	summaryMarkdownPath := filepath.Join(dir, "test-summary.md")
	summaryJSONPath := filepath.Join(dir, "test-summary.json")
	_, err := r.sanitizeAndParseArgs(summaryCommand, []string{
		"--junit-glob=" + filepath.Join(dir, "junit.*.xml"),
		"--summary-output-dir=" + dir,
	})
	require.NoError(t, err)

	require.NoError(t, r.generateSummary())
	body, err := os.ReadFile(summaryMarkdownPath)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(body), "<table>"))
	require.Contains(t, string(body), "TestAlpha")
	require.Contains(t, string(body), "TestBeta")

	jsonBody, err := os.ReadFile(summaryJSONPath)
	require.NoError(t, err)
	require.Contains(t, string(jsonBody), `"name": "TestAlpha"`)
	require.Contains(t, string(jsonBody), `"name": "TestBeta"`)
}

func TestRunnerPrintSummarySkipsEmptySummary(t *testing.T) {
	dir := t.TempDir()
	report := mustReadTestsuitesFixture(t, "testdata/junit-empty.xml")
	require.NoError(t, junit.Write(filepath.Join(dir, "junit.empty.xml"), report))

	r := newRunner()
	_, err := r.sanitizeAndParseArgs(summaryCommand, []string{
		"--junit-glob=" + filepath.Join(dir, "junit.*.xml"),
		"--summary-output-dir=" + dir,
	})
	require.NoError(t, err)

	require.NoError(t, r.generateSummary())
	require.NoFileExists(t, filepath.Join(dir, "test-summary.md"))
	require.NoFileExists(t, filepath.Join(dir, "test-summary.json"))
}

func TestRunnerRunTestsTargetedRetryAndPersistedHistory(t *testing.T) {
	dir := t.TempDir()
	id := testID{"example.com/tests", "TestSuite/TestFlaky"}
	results := []attemptResult{
		{
			packages: []packageResult{{
				name:       id.packageName,
				outcome:    packageFailed,
				executions: []testExecution{{id: id, outcome: testFailed, failure: failureEvidence{details: "flaky failure", actionable: true}}},
			}},
			process: processResult{state: processExited, exitCode: 1},
		},
		{
			packages: []packageResult{{
				name:       id.packageName,
				outcome:    packagePassed,
				executions: []testExecution{{id: id, outcome: testPassed}},
			}},
			process: processResult{state: processExited},
		},
	}
	var specs []attemptSpec
	r := newRunner()
	r.maxAttempts = 2
	r.coverProfilePath = filepath.Join(dir, "coverage.out")
	r.junitOutputPath = filepath.Join(dir, "junit.xml")
	r.executeAttempt = func(_ context.Context, spec attemptSpec) attemptResult {
		specs = append(specs, spec)
		return results[len(specs)-1]
	}

	exitCode, err := r.runTests(context.Background(), []string{"./...", "-run=old", "-args", "value"})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	require.Len(t, specs, 2)
	require.Equal(t, []string{"./...", "-run", "^TestSuite$/^TestFlaky$", "-args", "value"}, specs[1].args)

	report, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	require.NoError(t, junit.ValidateCounters(report))
	require.Contains(t, collectJUnitTestNames(report.Suites), "TestSuite/TestFlaky")
	require.Contains(t, collectJUnitTestNames(report.Suites), "TestSuite/TestFlaky (retry 1) (final)")
}

func TestRunnerRunTestsWritesBeforeMissingRetryValidation(t *testing.T) {
	dir := t.TempDir()
	id := testID{"example.com/tests", "TestMissing"}
	results := []attemptResult{
		{
			packages: []packageResult{{name: id.packageName, outcome: packageFailed, executions: []testExecution{{id: id, outcome: testFailed}}}},
			process:  processResult{state: processExited, exitCode: 1},
		},
		{
			packages: []packageResult{{name: id.packageName, outcome: packagePassed}},
			process:  processResult{state: processExited},
		},
	}
	r := newRunner()
	r.maxAttempts = 2
	r.coverProfilePath = filepath.Join(dir, "coverage.out")
	r.junitOutputPath = filepath.Join(dir, "junit.xml")
	r.executeAttempt = func(_ context.Context, _ attemptSpec) attemptResult {
		result := results[0]
		results = results[1:]
		return result
	}

	exitCode, err := r.runTests(context.Background(), []string{"./..."})
	require.Equal(t, 1, exitCode)
	require.EqualError(t, err, "expected targeted rerun was not observed: example.com/tests.TestMissing")
	report, readErr := junit.Read(r.junitOutputPath)
	require.NoError(t, readErr)
	require.NoError(t, junit.ValidateCounters(report))
	require.Contains(t, collectJUnitTestNames(report.Suites), "TestMissing")
}

func TestRunnerTotalTimeoutDoesNotDuplicateAbort(t *testing.T) {
	dir := t.TempDir()
	r := newRunner()
	r.coverProfilePath = filepath.Join(dir, "coverage.out")
	r.junitOutputPath = filepath.Join(dir, "junit.xml")
	r.executeAttempt = func(_ context.Context, _ attemptSpec) attemptResult {
		return attemptResult{
			packages: []packageResult{{
				name:       "example.com/tests",
				outcome:    packageIncomplete,
				executions: []testExecution{{id: testID{"example.com/tests", "TestIncomplete"}, outcome: testIncomplete}},
			}},
			process: processResult{state: processDeadlineExceeded, exitCode: 1, details: "total timeout"},
		}
	}

	exitCode, err := r.runTests(context.Background(), []string{"./..."})
	require.NoError(t, err)
	require.Equal(t, 1, exitCode)
	report, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	names := collectJUnitTestNames(report.Suites)
	require.Equal(t, []string{"testrunner.TotalTimeout"}, names)
}

func TestRunnerCoordinatesNonTargetedAttempts(t *testing.T) {
	failedExecutions := make([]testExecution, targetedRetryThreshold+1)
	for i := range failedExecutions {
		failedExecutions[i] = testExecution{
			id:      testID{"example.com/tests", fmt.Sprintf("TestFailure%02d", i)},
			outcome: testFailed,
		}
	}
	tests := []struct {
		name       string
		results    []attemptResult
		maxAttempt int
		wantCalls  int
		wantExit   int
	}{
		{
			name: "pass",
			results: []attemptResult{{
				packages: []packageResult{{name: "example.com/tests", outcome: packagePassed}},
				process:  processResult{state: processExited},
			}},
			maxAttempt: 3,
			wantCalls:  1,
		},
		{
			name: "package abort repeats scope",
			results: []attemptResult{
				{
					packages: []packageResult{{
						name:       "example.com/tests",
						outcome:    packageFailed,
						executions: []testExecution{{id: testID{"example.com/tests", "TestIncomplete"}, outcome: testIncomplete}},
					}},
					process: processResult{state: processExited, exitCode: 1},
				},
				{packages: []packageResult{{name: "example.com/tests", outcome: packagePassed}}, process: processResult{state: processExited}},
			},
			maxAttempt: 2,
			wantCalls:  2,
		},
		{
			name: "threshold repeats scope",
			results: []attemptResult{
				{
					packages: []packageResult{{name: "example.com/tests", outcome: packageFailed, executions: failedExecutions}},
					process:  processResult{state: processExited, exitCode: 1},
				},
				{packages: []packageResult{{name: "example.com/tests", outcome: packagePassed}}, process: processResult{state: processExited}},
			},
			maxAttempt: 2,
			wantCalls:  2,
		},
		{
			name: "test timeout stops",
			results: []attemptResult{{
				diagnostics: []diagnostic{{kind: diagnosticTimeout, summary: "test timed out", details: "timeout"}},
				process:     processResult{state: processExited, exitCode: 1},
			}},
			maxAttempt: 3,
			wantCalls:  1,
			wantExit:   1,
		},
		{
			name: "start failure stops",
			results: []attemptResult{{
				process: processResult{state: processStartFailed, exitCode: 1, details: "go executable missing"},
			}},
			maxAttempt: 3,
			wantCalls:  1,
			wantExit:   1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			results := slices.Clone(test.results)
			var specs []attemptSpec
			r := newRunner()
			r.maxAttempts = test.maxAttempt
			r.coverProfilePath = filepath.Join(dir, "coverage.out")
			r.junitOutputPath = filepath.Join(dir, "junit.xml")
			r.executeAttempt = func(_ context.Context, spec attemptSpec) attemptResult {
				specs = append(specs, spec)
				result := results[0]
				results = results[1:]
				return result
			}

			exitCode, err := r.runTests(context.Background(), []string{"./...", "-run=original"})
			require.NoError(t, err)
			require.Equal(t, test.wantExit, exitCode)
			require.Len(t, specs, test.wantCalls)
			for _, spec := range specs {
				require.Equal(t, []string{"./...", "-run=original"}, spec.args)
			}
			report, err := junit.Read(r.junitOutputPath)
			require.NoError(t, err)
			require.NoError(t, junit.ValidateCounters(report))
		})
	}
}

func TestRunnerReturnsFinalArtifactWriteFailure(t *testing.T) {
	r := newRunner()
	r.coverProfilePath = filepath.Join(t.TempDir(), "coverage.out")
	r.junitOutputPath = filepath.Join(t.TempDir(), "missing", "junit.xml")
	r.executeAttempt = func(context.Context, attemptSpec) attemptResult {
		return attemptResult{process: processResult{state: processExited}}
	}

	exitCode, err := r.runTests(context.Background(), []string{"./..."})
	require.Equal(t, 1, exitCode)
	require.ErrorContains(t, err, "failed to write JUnit report")
}

func TestRunnerRecoversFromIntermediateArtifactWriteFailure(t *testing.T) {
	root := t.TempDir()
	outputDir := filepath.Join(root, "reports")
	id := testID{"example.com/tests", "TestFlaky"}
	results := []attemptResult{
		{
			packages: []packageResult{{
				name:       id.packageName,
				outcome:    packageFailed,
				executions: []testExecution{{id: id, outcome: testFailed}},
			}},
			process: processResult{state: processExited, exitCode: 1},
		},
		{
			packages: []packageResult{{
				name:       id.packageName,
				outcome:    packagePassed,
				executions: []testExecution{{id: id, outcome: testPassed}},
			}},
			process: processResult{state: processExited},
		},
	}
	r := newRunner()
	r.maxAttempts = 2
	r.coverProfilePath = filepath.Join(root, "coverage.out")
	r.junitOutputPath = filepath.Join(outputDir, "junit.xml")
	r.executeAttempt = func(context.Context, attemptSpec) attemptResult {
		if len(results) == 1 {
			require.NoError(t, os.MkdirAll(outputDir, 0o755))
		}
		result := results[0]
		results = results[1:]
		return result
	}

	exitCode, err := r.runTests(context.Background(), []string{"./..."})
	require.NoError(t, err)
	require.Equal(t, 0, exitCode)
	report, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	require.Len(t, report.Suites, 2)
}
