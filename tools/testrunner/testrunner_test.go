package testrunner

import (
	"os"
	"path/filepath"
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

func TestWriteCurrentReport(t *testing.T) {
	r := newRunner()
	r.junitOutputPath = filepath.Join(t.TempDir(), "junit.xml")

	// Simulate attempt 1 completing with failures.
	a1 := r.newAttempt()
	a1.result = attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{{
				id:      testID{packageName: "example.com/tests", testName: "TestOne"},
				outcome: testFailed,
			}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	r.writeCurrentReport()

	result, err := junit.Read(r.junitOutputPath)
	require.NoError(t, err)
	require.Equal(t, 1, result.Failures)
	require.Len(t, result.Suites, 1)

	// Simulate attempt 2 also completing. The intermediate write should now
	// contain failures from both attempts, so that if the process is killed
	// before attempt 3 the file on disk already has the full picture.
	a2 := r.newAttempt()
	a2.result = attemptResult{
		packages: []packageResult{{
			name:    "example.com/tests",
			outcome: packageFailed,
			executions: []testExecution{{
				id:      testID{packageName: "example.com/tests", testName: "TestOne"},
				outcome: testFailed,
			}},
		}},
		process: processResult{state: processExited, exitCode: 1},
	}

	r.writeCurrentReport()

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

	r.reportCrash()
	report, err := junit.Read(out)
	require.NoError(t, err)
	require.Len(t, report.Suites, 1)
	require.Len(t, report.Suites[0].Testcases, 1)
	require.Equal(t, "my-test (crash)", report.Suites[0].Testcases[0].Name)
	require.NotNil(t, report.Suites[0].Testcases[0].Failure)
	require.Equal(t, string(failureTypeCrash), report.Suites[0].Testcases[0].Failure.Type)
	require.Equal(t, 1, report.Failures)
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
