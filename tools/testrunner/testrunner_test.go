package testrunner

import (
	"os"
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
	j1 := &junitReport{path: "testdata/junit-attempt-1.xml"}
	require.NoError(t, j1.read())
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
	j2 := &junitReport{path: "testdata/junit-attempt-2.xml"}
	require.NoError(t, j2.read())
	a2 := r.newAttempt()
	a2.junitReport = j2

	r.writeCurrentReport()

	result2 := &junitReport{path: out.Name()}
	require.NoError(t, result2.read())
	require.Equal(t, 4, result2.Failures) // 2 from attempt 1 + 2 from attempt 2
	require.Len(t, result2.Suites, 2)
}

func TestRunnerReportCrash(t *testing.T) {
	out, err := os.CreateTemp("", "junit-report-*.xml")
	require.NoError(t, err)
	defer func() { _ = os.Remove(out.Name()) }()

	r := newRunner()
	_, err = r.sanitizeAndParseArgs(crashReportCommand, []string{
		"--junitfile=" + out.Name(),
		"--crashreportname=my-test",
	})
	require.NoError(t, err)

	r.reportCrash()
	requireReportEquals(t, "testdata/junit-crash-output.xml", out.Name())
}
