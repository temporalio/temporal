package testrunner

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
			"--junitfile=test.xml",
			"-foo",
			"bar",
			// max-attempts has been stripped
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, args)
		require.Equal(t, "test.xml", r.junitOutputPath)
		require.Equal(t, 3, r.maxAttempts)
		require.Equal(t, "test.cover.out", r.coverProfilePath)
	})

	t.Run("TotalTimeoutDerivedFromGoTestTimeout", func(t *testing.T) {
		r := newRunner()
		args, err := r.sanitizeAndParseArgs(testCommand, []string{
			"--gotestsum-path=/bin/gotestsum",
			"--junitfile=test.xml",
			"--",
			"-timeout=35m",
			"-coverprofile=test.cover.out",
		})
		require.NoError(t, err)
		// The testrunner should derive its total deadline from the go test -timeout flag.
		require.Equal(t, 35*time.Minute, r.totalTimeout)
		// The flag must still be present in the passthrough args so gotestsum/go test
		// also honour it.
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

	t.Run("GoTestSumPathMissing", func(t *testing.T) {
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
		require.ErrorContains(t, err, `missing required argument "--gotestsum-path="`)
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
	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	r := newRunner()
	r.junitOutputPath = out.Name()

	// Simulate attempt 1 completing with failures.
	j1 := mustReadReportFixture(t, "testdata/junit-attempt-1.xml")
	a1 := r.newAttempt()
	a1.junitReport = j1

	r.writeCurrentReport()

	result := &junitReport{path: out.Name()}
	require.NoError(t, result.read())
	require.Equal(t, 2, result.Failures)
	require.Len(t, result.Suites, 1)

	// Simulate attempt 2 also completing. The intermediate write should now
	// contain failures from both attempts, so that if the process is killed
	// before attempt 3 the file on disk already has the full picture.
	j2 := mustReadReportFixture(t, "testdata/junit-attempt-2.xml")
	a2 := r.newAttempt()
	a2.junitReport = j2

	r.writeCurrentReport()

	result2 := &junitReport{path: out.Name()}
	require.NoError(t, result2.read())
	require.Equal(t, 4, result2.Failures) // 2 from attempt 1 + 2 from attempt 2
	require.Len(t, result2.Suites, 2)
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
	requireReportEquals(t, "testdata/junit-crash-output.xml", out)
}

func TestRunnerPrintSummary(t *testing.T) {
	dir := t.TempDir()
	report1 := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	report1.Suites[0].Name = "SuiteA"
	report1.Suites[0].Testcases[0].Name = "TestAlpha"
	report1.Suites[0].Testcases[0].Failure.Type = failureKindFailed
	report1.Suites[0].Testcases[0].Failure.Data = "alpha failure"
	report1.path = filepath.Join(dir, "junit.alpha.xml")
	require.NoError(t, report1.write())
	report2 := mustReadReportFixture(t, "testdata/junit-single-failure.xml")
	report2.Suites[0].Name = "SuiteB"
	report2.Suites[0].Testcases[0].Name = "TestBeta"
	report2.Suites[0].Testcases[0].Failure.Type = failureKindFailed
	report2.Suites[0].Testcases[0].Failure.Data = "beta failure"
	report2.path = filepath.Join(dir, "junit.beta.xml")
	require.NoError(t, report2.write())

	r := newRunner()
	_, err := r.sanitizeAndParseArgs(summaryCommand, []string{
		"--junit-glob=" + filepath.Join(dir, "junit.*.xml"),
	})
	require.NoError(t, err)

	stdout := os.Stdout
	rpipe, wpipe, err := os.Pipe()
	require.NoError(t, err)
	defer func() {
		os.Stdout = stdout
		require.NoError(t, rpipe.Close())
	}()

	os.Stdout = wpipe
	require.NoError(t, r.printSummary())
	require.NoError(t, wpipe.Close())

	body, err := io.ReadAll(rpipe)
	require.NoError(t, err)
	require.Equal(t, 1, strings.Count(string(body), "<table>"))
	require.Contains(t, string(body), "TestAlpha")
	require.Contains(t, string(body), "TestBeta")
}
