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

// Option configures shard selection.
type Option struct {
	salt string
}

// WithSalt sets the default sharding salt. A non-empty TEST_SHARD_SALT overrides it.
func WithSalt(salt string) Option {
	return Option{salt: salt}
}

// FromEnv reads TEST_TOTAL_SHARDS and the zero-based TEST_SHARD_INDEX.
// Sharding is disabled when either variable is empty.
// A non-empty TEST_SHARD_SALT overrides the supplied default salt.
// The salt is appended to the full test name before hashing with FarmHash Fingerprint32.
func FromEnv(options ...Option) (Shard, error) {
	var salt string
	for _, option := range options {
		salt = option.salt
	}
	totalStr := os.Getenv("TEST_TOTAL_SHARDS")
	indexStr := os.Getenv("TEST_SHARD_INDEX")
	if totalStr == "" || indexStr == "" {
		return Shard{}, nil
	}
	total, err := strconv.Atoi(totalStr)
	if err != nil {
		return Shard{}, fmt.Errorf("parsing TEST_TOTAL_SHARDS: %w", err)
	}
	if total < 1 {
		return Shard{}, fmt.Errorf("TEST_TOTAL_SHARDS must be positive: %d", total)
	}
	index, err := strconv.Atoi(indexStr)
	if err != nil {
		return Shard{}, fmt.Errorf("parsing TEST_SHARD_INDEX: %w", err)
	}
	if index < 0 || index >= total {
		return Shard{}, fmt.Errorf("TEST_SHARD_INDEX out of bounds: %d must be in [0, %d)", index, total)
	}
	return Shard{index: index, total: total, salt: cmp.Or(os.Getenv("TEST_SHARD_SALT"), salt)}, nil
}

// Contains reports whether the full test name belongs to this shard.
func (s Shard) Contains(name string) bool {
	return s.total == 0 || alreadySelected(name) || s.testIndex(name) == s.index
}

// MaybeSkip skips t if its full name belongs to another shard.
// Once selected, t and its descendants retain that decision across later sharding checks.
func (s Shard) MaybeSkip(t *testing.T) {
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
