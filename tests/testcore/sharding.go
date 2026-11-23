package testcore

import (
	_ "embed"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testshard"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

// FunctionalTestShard reads shard settings using the checked-in functional test salt.
func FunctionalTestShard(t *testing.T) testshard.Shard {
	t.Helper()
	shard, err := testshard.FromEnv(strings.TrimSpace(shardSalt))
	require.NoError(t, err)
	return shard
}
