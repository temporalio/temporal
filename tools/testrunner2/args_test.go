package testrunner2

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	// Not parallel: subtests use t.Setenv which modifies process environment.

	t.Run("defaults", func(t *testing.T) {
		cfg := defaultConfig()
		require.Equal(t, 1, cfg.maxAttempts)
		require.Equal(t, runtime.NumCPU(), cfg.parallelism)
		require.Equal(t, 1, cfg.totalShards)
		require.Equal(t, 0, cfg.shardIndex)
	})

	t.Run("reads sharding from env", func(t *testing.T) {
		t.Setenv("TEST_RUNNER_SHARDS_TOTAL", "4")
		t.Setenv("TEST_RUNNER_SHARD_INDEX", "2")

		cfg := defaultConfig()
		require.Equal(t, 4, cfg.totalShards)
		require.Equal(t, 2, cfg.shardIndex)
	})

	t.Run("reads workers from env", func(t *testing.T) {
		t.Setenv("TEST_RUNNER_WORKERS", "8")

		cfg := defaultConfig()
		require.Equal(t, 8, cfg.parallelism)
	})

	t.Run("ignores invalid env values", func(t *testing.T) {
		t.Setenv("TEST_RUNNER_SHARDS_TOTAL", "notanumber")
		t.Setenv("TEST_RUNNER_SHARD_INDEX", "-1")
		t.Setenv("TEST_RUNNER_WORKERS", "0")

		cfg := defaultConfig()
		require.Equal(t, 1, cfg.totalShards)
		require.Equal(t, 0, cfg.shardIndex)
		require.Equal(t, runtime.NumCPU(), cfg.parallelism)
	})
}

func TestParseArgs(t *testing.T) {
	t.Parallel()

	t.Run("strips known flags and passes through the rest", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		args, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"--group-by=test",
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
		require.Equal(t, GroupByTest, cfg.groupBy)
	})

	t.Run("rejects zero max attempts", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=0", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --max-attempts: must be greater than zero`)
	})

	t.Run("rejects non-numeric max attempts", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=invalid", // invalid!
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --max-attempts: strconv.Atoi: parsing "invalid"`)
	})

	t.Run("requires junitfile", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			// missing: "--junitfile=test.xml"
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			"-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--junitfile"`)
	})

	t.Run("requires coverprofile", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-foo",
			"bar",
			"--max-attempts=3",
			"--",
			// missing: "-coverprofile=test.cover.out",
			"baz",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "-coverprofile"`)
	})

	t.Run("requires log dir", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"-coverprofile=test.cover.out",
			// missing: "--log-dir=/tmp/logs"
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--log-dir"`)
	})

	t.Run("accepts valid group-by modes", func(t *testing.T) {
		for _, mode := range []string{"test", "none"} {
			cfg := defaultConfig()
			cfg.log = func(string, ...any) {}
			_, err := parseArgs(testCommand, []string{
				"--junitfile=test.xml",
				"--log-dir=/tmp/logs",
				"-coverprofile=test.cover.out",
				"--group-by=" + mode,
			}, &cfg)
			require.NoError(t, err)
			require.Equal(t, GroupMode(mode), cfg.groupBy)
		}
	})

	t.Run("rejects invalid group-by mode", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-coverprofile=test.cover.out",
			"--group-by=invalid",
		}, &cfg)
		require.ErrorContains(t, err, `invalid argument --group-by: must be 'test' or 'none'`)
	})

	t.Run("requires group-by", func(t *testing.T) {
		cfg := defaultConfig()
		cfg.log = func(string, ...any) {}
		_, err := parseArgs(testCommand, []string{
			"--junitfile=test.xml",
			"--log-dir=/tmp/logs",
			"-coverprofile=test.cover.out",
		}, &cfg)
		require.ErrorContains(t, err, `missing required argument "--group-by"`)
	})
}

func TestParseTestArgs(t *testing.T) {
	t.Parallel()

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
