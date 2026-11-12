package testshard

import (
	_ "embed"
	"strings"
	"testing"

	sharedtestshard "go.temporal.io/testx/testshard"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

// FromEnv reads shard configuration with the default salt for server functional tests.
func FromEnv(t *testing.T) sharedtestshard.Shard {
	t.Helper()
	shard, err := sharedtestshard.FromEnv(sharedtestshard.WithSalt(strings.TrimSpace(shardSalt)))
	if err != nil {
		t.Fatal(err)
	}
	return shard
}
