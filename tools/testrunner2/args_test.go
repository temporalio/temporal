package testrunner2

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseConfig(t *testing.T) {
	t.Run("Passthrough", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		args, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"--group-by=file",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.NoError(t, err)
		require.Equal(t, []string{
			"--junitfile=test.xml",
			"-foo",
			"bar",
			// max-attempts, log-dir, and group-by have been stripped
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, args)
		require.Equal(t, "test.xml", cfg.junitReportPath)
		require.Equal(t, 3, cfg.maxAttempts)
		require.Equal(t, "test.cover.out", cfg.coverProfilePath)
		require.Equal(t, "/tmp/logs", cfg.logDir)
		require.Equal(t, GroupByFile, cfg.groupBy)
	})

	t.Run("AttemptsInvalid1", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=0", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --max-attempts=: must be greater than zero`)
	})

	t.Run("AttemptsInvalid2", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=invalid", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --max-attempts=: strconv.Atoi: parsing "invalid"`)
	})

	t.Run("JunitfileMissing", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			// missing: "--junitfile=test.xml"
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--junitfile="`)
	})

	t.Run("CoverprofileMissing", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			// missing: "-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "-coverprofile="`)
	})

	t.Run("LogDirMissing", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"-coverprofile=test.cover.out",
			// missing: "--log-dir=/tmp/logs"
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--log-dir="`)
	})

	t.Run("GroupByValid", func(t *testing.T) {
		for _, mode := range []string{"package", "file", "suite", "none"} {
			cfg := defaultConfig()
			cfg.log = func(string, ...any) {}
			_, err := parseConfig(testCommand, []string{
				"--junitfile=test.xml",
				"--log-dir=/tmp/logs",
				"-coverprofile=test.cover.out",
				"--group-by=" + mode,
			}, &cfg)
			require.NoError(t, err)
			require.Equal(t, GroupMode(mode), cfg.groupBy)
		}
	})

	t.Run("GroupByInvalid", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-coverprofile=test.cover.out",
			"--group-by=invalid",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --group-by=: must be 'package', 'file', 'suite', or 'none'`)
	})

	t.Run("GroupByMissing", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseConfig(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-coverprofile=test.cover.out",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--group-by="`)
	})
}

func TestParseTestArgs(t *testing.T) {
	t.Run("separates dirs from flags", func(t *testing.T) {
		dirs, baseArgs, testBinaryArgs := parseTestArgs([]string{
			"-race",
			"./pkg/foo",
			"./pkg/bar",
			"-cover",
		})
		require.Equal(t, []string{"./pkg/foo", "./pkg/bar"}, dirs)
		require.Equal(t, []string{"-race", "-cover"}, baseArgs)
		require.Empty(t, testBinaryArgs)
	})

	t.Run("handles -args flag", func(t *testing.T) {
		dirs, baseArgs, testBinaryArgs := parseTestArgs([]string{
			"./pkg/foo",
			"-args",
			"-custom-flag",
			"-persistenceType=sql",
		})
		require.Equal(t, []string{"./pkg/foo"}, dirs)
		require.Empty(t, baseArgs)
		require.Equal(t, []string{"-custom-flag", "-persistenceType=sql"}, testBinaryArgs)
	})

	t.Run("handles absolute paths", func(t *testing.T) {
		dirs, baseArgs, testBinaryArgs := parseTestArgs([]string{
			"/abs/path/to/pkg",
			"-race",
		})
		require.Equal(t, []string{"/abs/path/to/pkg"}, dirs)
		require.Equal(t, []string{"-race"}, baseArgs)
		require.Empty(t, testBinaryArgs)
	})
}
