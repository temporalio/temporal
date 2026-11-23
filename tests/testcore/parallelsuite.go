package testcore

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/parallelsuite"
	"go.temporal.io/testx/testshard"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

// Run runs a functional suite, selecting methods for the configured shard before suite setup.
func Run[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.RunInShard(t, functionalTestShard(t), s, args...)
}

// RunLegacySequential runs a functional suite's selected methods sequentially.
//
// Deprecated: use [Run] for new tests.
func RunLegacySequential[T parallelsuite.TestingSuite](t *testing.T, s T, args ...any) {
	t.Helper()
	parallelsuite.RunLegacySequentialInShard(t, functionalTestShard(t), s, args...) //nolint:staticcheck // SA1019: forwards to the legacy runner.
}

func functionalTestShard(t *testing.T) testshard.Shard {
	t.Helper()
	shard, err := testshard.FromEnv(strings.TrimSpace(shardSalt))
	require.NoError(t, err)
	return shard
}
