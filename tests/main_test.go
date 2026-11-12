package tests

import (
	_ "embed"
	"strings"
	"testing"

	"go.temporal.io/testx/testshard"
)

//go:embed testcore/shard_salt.txt
var functionalTestShardSalt string

func TestMain(m *testing.M) {
	// The salt must be configured before parallelsuite selects test methods.
	testshard.SetDefaults(testshard.WithSalt(strings.TrimSpace(functionalTestShardSalt)))
	m.Run()
}
