package tests

import (
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"
)

//go:embed testcore/shard_salt.txt
var functionalTestShardSalt string

func TestMain(m *testing.M) {
	// The salt must be configured before parallelsuite selects test methods.
	if os.Getenv("TEST_SHARD_SALT") == "" {
		if err := os.Setenv("TEST_SHARD_SALT", strings.TrimSpace(functionalTestShardSalt)); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	m.Run()
}
