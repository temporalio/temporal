package testshard

import (
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromEnv(t *testing.T) {
	t.Setenv("TEST_SHARD_SALT", "")
	for _, tc := range []struct {
		name    string
		total   string
		index   string
		salt    string
		want    Shard
		wantErr string
	}{
		{name: "unset"},
		{name: "total missing", index: "1"},
		{name: "index missing", total: "4"},
		{name: "one shard", total: "1", index: "0", salt: "my-suite", want: Shard{total: 1, salt: "my-suite"}},
		{name: "first shard", total: "4", index: "0", salt: "my-suite", want: Shard{total: 4, salt: "my-suite"}},
		{name: "last shard", total: "4", index: "3", salt: "my-suite", want: Shard{index: 3, total: 4, salt: "my-suite"}},
		{name: "missing salt", total: "4", index: "0", wantErr: "sharding salt is required: set TEST_SHARD_SALT or use WithSalt"},
		{name: "malformed total", total: "four", index: "0", wantErr: `parsing TEST_TOTAL_SHARDS: strconv.Atoi: parsing "four": invalid syntax`},
		{name: "zero total", total: "0", index: "0", wantErr: "TEST_TOTAL_SHARDS must be positive: 0"},
		{name: "negative total", total: "-1", index: "0", wantErr: "TEST_TOTAL_SHARDS must be positive: -1"},
		{name: "malformed index", total: "4", index: "first", wantErr: `parsing TEST_SHARD_INDEX: strconv.Atoi: parsing "first": invalid syntax`},
		{name: "negative index", total: "4", index: "-1", wantErr: "TEST_SHARD_INDEX out of bounds: -1 must be in [0, 4)"},
		{name: "index equal to total", total: "4", index: "4", wantErr: "TEST_SHARD_INDEX out of bounds: 4 must be in [0, 4)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TEST_TOTAL_SHARDS", tc.total)
			t.Setenv("TEST_SHARD_INDEX", tc.index)
			t.Setenv("TEST_SHARD_SALT", tc.salt)
			shard, err := FromEnv()
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, shard)
		})
	}
}

func TestSkipUnlessInShardInheritsSelection(t *testing.T) {
	t.Parallel()
	selected := Shard{total: 1, salt: "selected-salt"}
	other := Shard{total: 4, index: 0, salt: "other-salt"}
	t.Run("suite", func(t *testing.T) {
		selected.SkipUnlessInShard(t)
		t.Run("nested", func(t *testing.T) {
			t.Cleanup(func() { require.False(t, t.Skipped(), "a selected parent's descendants must stay on its shard") })
			other.SkipUnlessInShard(t)
		})
		t.Cleanup(func() { require.False(t, t.Skipped(), "the first sharding decision must be retained") })
		other.SkipUnlessInShard(t)
	})
	require.False(t, alreadySelected(t.Name()+"/suite"), "selection must be removed when its test finishes")
}

func TestFromEnvCustomVariables(t *testing.T) {
	t.Setenv("TEST_TOTAL_SHARDS", "invalid")
	t.Setenv("TEST_SHARD_INDEX", "invalid")
	t.Setenv("TEST_SHARD_SALT", "ignored")
	for _, tc := range []struct {
		name        string
		total       string
		index       string
		salt        string
		defaultSalt string
		want        Shard
		wantErr     string
	}{
		{name: "custom salt", total: "4", index: "1", salt: "-test-salt", want: Shard{index: 1, total: 4, salt: "-test-salt"}},
		{name: "default salt", total: "4", index: "2", defaultSalt: "-salt-2827", want: Shard{index: 2, total: 4, salt: "-salt-2827"}},
		{name: "missing salt", total: "4", index: "0", wantErr: "sharding salt is required: set CI_SHARD_SALT or use WithSalt"},
		{name: "missing total", index: "1"},
		{name: "invalid total", total: "four", index: "0", wantErr: `parsing CI_TOTAL_SHARDS: strconv.Atoi: parsing "four": invalid syntax`},
		{name: "zero total", total: "0", index: "0", wantErr: "CI_TOTAL_SHARDS must be positive: 0"},
		{name: "invalid index", total: "4", index: "first", wantErr: `parsing CI_SHARD_INDEX: strconv.Atoi: parsing "first": invalid syntax`},
		{name: "index out of bounds", total: "4", index: "4", wantErr: "CI_SHARD_INDEX out of bounds: 4 must be in [0, 4)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("CI_TOTAL_SHARDS", tc.total)
			t.Setenv("CI_SHARD_INDEX", tc.index)
			t.Setenv("CI_SHARD_SALT", tc.salt)
			shard, err := FromEnv(
				WithSalt(tc.defaultSalt),
				WithTotalShardsEnv("CI_TOTAL_SHARDS"),
				WithShardIndexEnv("CI_SHARD_INDEX"),
				WithSaltEnv("CI_SHARD_SALT"),
			)
			if tc.wantErr != "" {
				require.EqualError(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, shard)
		})
	}
}

func TestSkipUnlessInShardProcess(t *testing.T) {
	if os.Getenv("TESTX_SKIP_PROCESS") == "" {
		return
	}
	var options []Option
	if salt := os.Getenv("TESTX_SKIP_SALT"); salt != "" {
		options = append(options, WithSalt(salt))
	}
	SkipUnlessInShard(t, options...)
	t.Log("test body ran")
}

func TestSkipUnlessInShard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		total   string
		index   string
		salt    string
		envSalt string
		wantRun bool
		wantErr string
	}{
		{name: "disabled", wantRun: true},
		{name: "owns test", total: "4", index: "0", envSalt: "my-suite", wantRun: true},
		{name: "other shard", total: "4", index: "1", envSalt: "my-suite"},
		{name: "default salt", total: "4", index: "2", salt: "other", wantRun: true},
		{name: "missing salt", total: "4", index: "0", wantErr: "sharding salt is required: set TEST_SHARD_SALT or use WithSalt"},
		{name: "misconfigured", total: "0", index: "0", wantErr: "TEST_TOTAL_SHARDS must be positive: 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestSkipUnlessInShardProcess$", "-test.v")
			cmd.Env = append(os.Environ(), "TESTX_SKIP_PROCESS=1", "TESTX_SKIP_SALT="+tc.salt,
				"TEST_TOTAL_SHARDS="+tc.total, "TEST_SHARD_INDEX="+tc.index, "TEST_SHARD_SALT="+tc.envSalt)
			output, err := cmd.CombinedOutput()
			if tc.wantErr != "" {
				require.Error(t, err, "%s", output)
				require.Contains(t, string(output), tc.wantErr)
				require.NotContains(t, string(output), "panic:")
				return
			}
			require.NoError(t, err, "%s", output)
			if tc.wantRun {
				require.Contains(t, string(output), "test body ran")
			} else {
				require.NotContains(t, string(output), "test body ran")
			}
		})
	}
}

func TestSetDefaultsProcess(t *testing.T) {
	if os.Getenv("TESTX_DEFAULTS_PROCESS") == "" {
		return
	}
	SetDefaults(
		WithSalt("process-salt"),
		WithTotalShardsEnv("CI_TOTAL_SHARDS"),
		WithShardIndexEnv("CI_SHARD_INDEX"),
		WithSaltEnv("CI_SHARD_SALT"),
	)
	want := Shard{index: 1, total: 4, salt: "process-salt"}
	shard, err := FromEnv()
	require.NoError(t, err)
	require.Equal(t, want, shard)

	shard, err = FromEnv(WithSalt("suite-salt"))
	require.NoError(t, err)
	require.Equal(t, Shard{index: 1, total: 4, salt: "suite-salt"}, shard)
	shard, err = FromEnv()
	require.NoError(t, err)
	require.Equal(t, want, shard)

	t.Setenv("CI_SHARD_SALT", "env-salt")
	shard, err = FromEnv(WithSalt("suite-salt"))
	require.NoError(t, err)
	require.Equal(t, Shard{index: 1, total: 4, salt: "env-salt"}, shard)

	t.Setenv("CI_SHARD_INDEX", "3")
	shard, err = FromEnv()
	require.NoError(t, err)
	require.Equal(t, Shard{index: 3, total: 4, salt: "env-salt"}, shard)

	SetDefaults(WithSalt("updated-salt"))
	t.Setenv("CI_SHARD_SALT", "")
	shard, err = FromEnv()
	require.NoError(t, err)
	require.Equal(t, Shard{index: 3, total: 4, salt: "updated-salt"}, shard)
}

func TestSetDefaults(t *testing.T) {
	t.Parallel()
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestSetDefaultsProcess$")
	cmd.Env = append(os.Environ(), "TESTX_DEFAULTS_PROCESS=1",
		"TEST_TOTAL_SHARDS=invalid", "TEST_SHARD_INDEX=invalid", "TEST_SHARD_SALT=ignored",
		"CI_TOTAL_SHARDS=4", "CI_SHARD_INDEX=1", "CI_SHARD_SALT=")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
}
