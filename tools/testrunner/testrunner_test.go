package testrunner

import (
	"os"
	"testing"

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
