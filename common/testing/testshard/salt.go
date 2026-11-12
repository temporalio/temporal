package testshard

import (
	_ "embed"
	"strings"
)

// shardSalt is used to distribute functional tests across shards.
// This value is automatically updated by the optimize-test-sharding workflow.
//
//go:embed shard_salt.txt
var shardSalt string

// Salt returns the default salt for server functional tests.
func Salt() string {
	return strings.TrimSpace(shardSalt)
}
