package testshard

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFromEnv(t *testing.T) {
	t.Setenv("TEST_SHARD_SALT", "")
	for _, tc := range []struct {
		name    string
		total   string
		index   string
		wantErr string
	}{
		{name: "unset"},
		{name: "total missing", index: "1"},
		{name: "index missing", total: "4"},
		{name: "one shard", total: "1", index: "0"},
		{name: "first shard", total: "4", index: "0"},
		{name: "last shard", total: "4", index: "3"},
		{name: "malformed total", total: "four", index: "0", wantErr: "TEST_TOTAL_SHARDS"},
		{name: "zero total", total: "0", index: "0", wantErr: "TEST_TOTAL_SHARDS"},
		{name: "negative total", total: "-1", index: "0", wantErr: "TEST_TOTAL_SHARDS"},
		{name: "malformed index", total: "4", index: "first", wantErr: "TEST_SHARD_INDEX"},
		{name: "negative index", total: "4", index: "-1", wantErr: "TEST_SHARD_INDEX"},
		{name: "index equal to total", total: "4", index: "4", wantErr: "TEST_SHARD_INDEX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("TEST_TOTAL_SHARDS", tc.total)
			t.Setenv("TEST_SHARD_INDEX", tc.index)
			shard, err := FromEnv("")
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			if tc.total == "" || tc.index == "" || tc.total == "1" {
				require.True(t, shard.Contains("TestSuite/TestMethod"))
			}
		})
	}
}

func TestContainsCompatibility(t *testing.T) {
	t.Setenv("TEST_SHARD_SALT", "")
	for _, tc := range []struct {
		name  string
		salt  string
		owner string
	}{
		{name: "TestSuite/TestMethod", owner: "1"},
		{name: "TestSuite/TestMethod", salt: "-salt-2827", owner: "2"},
		{name: "TestSuite/TestOther", salt: "-salt-2827", owner: "0"},
	} {
		t.Run(tc.name+tc.salt, func(t *testing.T) {
			t.Setenv("TEST_TOTAL_SHARDS", "4")
			for _, index := range []string{"0", "1", "2", "3"} {
				t.Setenv("TEST_SHARD_INDEX", index)
				shard, err := FromEnv(tc.salt)
				require.NoError(t, err)
				require.Equal(t, index == tc.owner, shard.Contains(tc.name))
			}
		})
	}
}

func TestMaybeSkipInheritsSelection(t *testing.T) {
	t.Parallel()
	selected := Shard{total: 1}
	other := Shard{total: 4, index: 0, salt: "other-salt"}
	t.Run("suite", func(t *testing.T) {
		selected.MaybeSkip(t)
		t.Run("nested", func(t *testing.T) {
			t.Cleanup(func() { require.False(t, t.Skipped(), "a selected parent's descendants must stay on its shard") })
			other.MaybeSkip(t)
		})
		t.Cleanup(func() { require.False(t, t.Skipped(), "the first sharding decision must be retained") })
		other.MaybeSkip(t)
	})
	require.False(t, other.Contains(t.Name()+"/suite"), "selection must be removed when its test finishes")
}
