// Package testshard distributes tests across CI shards using full test names and a salt.
package testshard

import (
	"cmp"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/dgryski/go-farm"
)

// Selections live until their owning test's cleanup, including all of its subtests.
var selectedTests sync.Map

// Shard selects tests for one shard. Its zero value selects every test.
type Shard struct {
	index int
	total int
	salt  string
}

type config struct {
	salt           string
	totalShardsEnv string
	shardIndexEnv  string
	saltEnv        string
}

// Option configures shard selection.
type Option func(*config)

// WithSalt sets the default sharding salt. A non-empty salt environment variable overrides it.
func WithSalt(salt string) Option {
	return func(cfg *config) { cfg.salt = salt }
}

// WithTotalShardsEnv sets the variable used for the shard count; the default is TEST_TOTAL_SHARDS.
func WithTotalShardsEnv(name string) Option {
	return func(cfg *config) { cfg.totalShardsEnv = name }
}

// WithShardIndexEnv sets the variable used for the zero-based index; the default is TEST_SHARD_INDEX.
func WithShardIndexEnv(name string) Option {
	return func(cfg *config) { cfg.shardIndexEnv = name }
}

// WithSaltEnv sets the variable used for the salt override; the default is TEST_SHARD_SALT.
func WithSaltEnv(name string) Option {
	return func(cfg *config) { cfg.saltEnv = name }
}

// FromEnv reads TEST_TOTAL_SHARDS and the zero-based TEST_SHARD_INDEX by default.
// Sharding is disabled when either configured variable is empty.
// A non-empty salt is required when sharding is enabled.
// A non-empty salt environment variable overrides the salt supplied with WithSalt.
// The salt is appended to the full test name before hashing with FarmHash Fingerprint32.
func FromEnv(options ...Option) (Shard, error) {
	cfg := config{
		totalShardsEnv: "TEST_TOTAL_SHARDS",
		shardIndexEnv:  "TEST_SHARD_INDEX",
		saltEnv:        "TEST_SHARD_SALT",
	}
	for _, option := range options {
		option(&cfg)
	}
	totalStr := os.Getenv(cfg.totalShardsEnv)
	indexStr := os.Getenv(cfg.shardIndexEnv)
	if totalStr == "" || indexStr == "" {
		return Shard{}, nil
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil {
		return Shard{}, fmt.Errorf("parsing %s: %w", cfg.totalShardsEnv, err)
	}
	if total < 1 {
		return Shard{}, fmt.Errorf("%s must be positive: %d", cfg.totalShardsEnv, total)
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return Shard{}, fmt.Errorf("parsing %s: %w", cfg.shardIndexEnv, err)
	}
	if index < 0 || index >= total {
		return Shard{}, fmt.Errorf("%s out of bounds: %d must be in [0, %d)", cfg.shardIndexEnv, index, total)
	}
	salt := cmp.Or(os.Getenv(cfg.saltEnv), cfg.salt)
	if salt == "" {
		return Shard{}, fmt.Errorf("sharding salt is required: set %s or use WithSalt", cfg.saltEnv)
	}
	return Shard{index: index, total: total, salt: salt}, nil
}

// SkipUnlessInShard reads shard configuration and skips t if it belongs to another shard.
// Invalid configuration fails the test.
func SkipUnlessInShard(t *testing.T, options ...Option) {
	t.Helper()
	shard, err := FromEnv(options...)
	if err != nil {
		t.Fatal(err)
	}
	shard.SkipUnlessInShard(t)
}

// SkipUnlessInShard skips t if its full name belongs to another shard.
// Once selected, t and its descendants retain that decision across later sharding checks.
func (s Shard) SkipUnlessInShard(t *testing.T) {
	t.Helper()
	if s.total == 0 || alreadySelected(t.Name()) {
		return
	}
	index := s.testIndex(t.Name())
	if index != s.index {
		t.Skipf("Skipping %s in test shard %d/%d (it runs in %d)", t.Name(), s.index+1, s.total, index+1)
	}
	selectedTests.Store(t.Name(), struct{}{})
	t.Cleanup(func() { selectedTests.Delete(t.Name()) })
	t.Logf("Running %s in test shard %d/%d", t.Name(), s.index+1, s.total)
}

func (s Shard) testIndex(name string) int {
	return int(uint64(farm.Fingerprint32([]byte(name+s.salt))) % uint64(s.total))
}

func alreadySelected(name string) bool {
	for {
		if _, ok := selectedTests.Load(name); ok {
			return true
		}
		index := strings.LastIndexByte(name, '/')
		if index < 0 {
			return false
		}
		name = name[:index]
	}
}
