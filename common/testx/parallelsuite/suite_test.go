package parallelsuite

import (
	"context"
	"flag"
	"os"
	"os/exec"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/testcontext"
	"go.temporal.io/testx/testshard"
)

type validSuite struct{ Suite[*validSuite] }

func (s *validSuite) TestA() {
	s.NotNil(s.T())

	ch := make(chan int, 1)
	s.Snd(ch, 42)
	s.Equal(42, s.Rcv(ch))
}

type validWithArgsSuite struct{ Suite[*validWithArgsSuite] }

func (s *validWithArgsSuite) TestA(name string, count int) {
	s.Equal("hello", name)
	s.Equal(42, count)
}

type noTestMethodsSuite struct{ Suite[*noTestMethodsSuite] }

type wrongSigSuite struct{ Suite[*wrongSigSuite] }

func (s *wrongSigSuite) TestBad(t *testing.T) {} //nolint:unused

type badNameTests struct{ Suite[*badNameTests] }

func (s *badNameTests) TestA() {} //nolint:unused

type exportedNonTestSuite struct{ Suite[*exportedNonTestSuite] }

func (s *exportedNonTestSuite) TestA()  {}
func (s *exportedNonTestSuite) Helper() {} //nolint:unused

type hasExtraFieldsSuite struct {
	Suite[*hasExtraFieldsSuite]
	x int //nolint:unused
}

func (s *hasExtraFieldsSuite) TestA() {} //nolint:unused

type setupTestSuite struct{ Suite[*setupTestSuite] }

func (s *setupTestSuite) TestA()     {}
func (s *setupTestSuite) SetupTest() {} //nolint:unused

type awaitTrueSuite struct{ Suite[*awaitTrueSuite] }

func (s *awaitTrueSuite) TestAwaitTrue() {
	var attempts atomic.Int32
	s.AwaitTrue(func() bool {
		attempts.Add(1)
		return true
	}, time.Second, time.Millisecond)
	s.Equal(int32(1), attempts.Load())
}

func (s *awaitTrueSuite) TestAwaitTrueFalseRetry() {
	var attempts atomic.Int32
	s.AwaitTrue(func() bool {
		return attempts.Add(1) == 2
	}, time.Second, time.Millisecond)
	s.Equal(int32(2), attempts.Load())
}

func (s *awaitTrueSuite) TestAwaitTruef() {
	s.AwaitTruef(func() bool {
		return true
	}, time.Second, time.Millisecond, "condition should pass")
}

type contextSuite struct{ Suite[*contextSuite] }

func (s *contextSuite) TestContextHasDeadline() {
	deadline, ok := s.Context().Deadline()
	s.True(ok)
	s.Positive(time.Until(deadline))
}

func (s *contextSuite) TestAwaitUsesSuiteContext() {
	type key struct{}

	testcontext.AttachDecorator(s.T(), key{}, func(ctx context.Context) context.Context {
		return context.WithValue(ctx, key{}, "decorated")
	})

	s.Await(func(s *contextSuite) {
		s.Equal("decorated", s.Context().Value(key{}))
		deadline, ok := s.Context().Deadline()
		s.True(ok)
		s.Less(time.Until(deadline), 200*time.Millisecond)
	}, 100*time.Millisecond, time.Millisecond)
}

type sealAfterRunSuite struct{ Suite[*sealAfterRunSuite] }

func (s *sealAfterRunSuite) TestAssertionAfterRun() {
	// Calling Run seals the parent's assertions and T().
	s.Run("subtest", func(s *sealAfterRunSuite) {
		s.NotNil(s.T()) // subtest assertions work fine
	})

	t := s.guardT.T

	// After Run: even passing assertions panic.
	require.Panics(t, func() { s.NotNil(t) })

	// T() also panics after Run.
	require.Panics(t, func() { s.T() })

	// Context() resolves through T() and also panics after Run.
	require.Panics(t, func() { s.Context() })

	// Channel helpers resolve through T() and also panic after Run.
	ch := make(chan int, 1)
	require.Panics(t, func() { s.Snd(ch, 42) })
	require.Panics(t, func() { s.Rcv(ch) })
}

func TestRun_AcceptsSuite(t *testing.T) {
	t.Run("no args", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &validSuite{}) })
	})
	t.Run("with args", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &validWithArgsSuite{}, "hello", 42) })
	})
	t.Run("legacy", func(t *testing.T) {
		require.NotPanics(t, func() { RunLegacySequential(t, &validSuite{}) }) //nolint:staticcheck // SA1019: validating deprecated legacy runner
	})
	t.Run("await true", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &awaitTrueSuite{}) })
	})
	t.Run("context", func(t *testing.T) {
		require.NotPanics(t, func() { Run(t, &contextSuite{}) })
	})
}

func TestRun_RejectsSuite(t *testing.T) {
	t.Run("no Test methods", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &noTestMethodsSuite{}) })
	})
	t.Run("wrong method signature", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &wrongSigSuite{}) })
	})
	t.Run("extra fields", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &hasExtraFieldsSuite{}) })
	})
	t.Run("name not ending in Suite", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &badNameTests{}) })
	})
	t.Run("non-Test exported method", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &exportedNonTestSuite{}) })
	})
	t.Run("SetupTest forbidden", func(t *testing.T) {
		require.Panics(t, func() { Run(t, &setupTestSuite{}) })
	})
}

type multiMethodSuite struct{ Suite[*multiMethodSuite] }

func (s *multiMethodSuite) TestFoo() {}
func (s *multiMethodSuite) TestBar() {}
func (s *multiMethodSuite) TestBaz() {}

func TestApplyTestifyMFilter(t *testing.T) {
	typ := reflect.TypeFor[*multiMethodSuite]()
	methods := discoverTestMethods(typ, typ.Elem(), nil)

	setFlag := func(t *testing.T, pattern string) {
		t.Helper()
		require.NoError(t, flag.Set("testify.m", pattern))
		t.Cleanup(func() { _ = flag.Set("testify.m", "") })
	}

	t.Run("no filter", func(t *testing.T) {
		require.Equal(t, methods, applyTestifyMFilter(methods))
	})
	t.Run("exact match", func(t *testing.T) {
		setFlag(t, "^TestBar$")
		filtered := applyTestifyMFilter(methods)
		require.Len(t, filtered, 1)
		require.Equal(t, "TestBar", filtered[0].Name)
	})
	t.Run("prefix match", func(t *testing.T) {
		setFlag(t, "^TestBa")
		filtered := applyTestifyMFilter(methods)
		require.Len(t, filtered, 2)
	})
	t.Run("no match", func(t *testing.T) {
		setFlag(t, "^TestNope$")
		require.Empty(t, applyTestifyMFilter(methods))
	})
}

func TestGuardSeal(t *testing.T) {
	t.Run("assertion after Run", func(t *testing.T) {
		Run(t, &sealAfterRunSuite{})
	})
}

type shardingSuite struct{ Suite[*shardingSuite] }

func (s *shardingSuite) TestA(ran chan string) {
	s.Snd(ran, "TestA")
	s.Run("nested", func(s *shardingSuite) { s.Snd(ran, "nested") })
}
func (s *shardingSuite) TestB(ran chan string) { s.Snd(ran, "TestB") }
func (s *shardingSuite) TestC(ran chan string) { s.Snd(ran, "TestC") }

func TestRunShardingProcess(t *testing.T) {
	if os.Getenv("TESTX_SHARD_PROCESS") == "" {
		return
	}
	ran := make(chan string, 4)
	t.Cleanup(func() {
		close(ran)
		got := make([]string, 0)
		for name := range ran {
			got = append(got, name)
		}
		slices.Sort(got)
		require.Equal(t, strings.Fields(os.Getenv("TESTX_SHARD_WANT")), got)
	})
	t.Run("suite", func(t *testing.T) {
		args := []any{ran}
		if os.Getenv("TESTX_SHARD_DEFAULT_SALT") != "" {
			shard, err := testshard.FromEnv(testshard.WithSalt(os.Getenv("TESTX_SHARD_DEFAULT_SALT")))
			require.NoError(t, err)
			args = append([]any{WithShard(shard)}, args...)
		}
		if os.Getenv("TESTX_SHARD_DISABLED") == "true" {
			args = append([]any{WithShard(testshard.Shard{})}, args...)
		}
		if os.Getenv("TESTX_SHARD_LEGACY") == "true" {
			RunLegacySequential(t, &shardingSuite{}, args...) //nolint:staticcheck // SA1019: sharding must also work in the legacy runner.
		} else {
			Run(t, &shardingSuite{}, args...)
		}
	})
}

func TestRunSharding(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		total       string
		index       string
		salt        string
		defaultSalt string
		legacy      bool
		disabled    bool
		filter      string
		want        string
		wantErr     string
	}{
		{name: "disabled", want: "TestA TestB TestC nested"},
		{name: "explicit zero shard", total: "invalid", index: "invalid", disabled: true, want: "TestA TestB TestC nested"},
		{name: "one shard", total: "1", index: "0", salt: "-test-salt", want: "TestA TestB TestC nested"},
		{name: "owns nested test", total: "4", index: "2", salt: "-test-salt", want: "TestA nested"},
		{name: "last shard", total: "4", index: "3", salt: "-test-salt", want: "TestB"},
		{name: "empty shard", total: "4", index: "0", salt: "-test-salt"},
		{name: "default salt", total: "4", index: "2", defaultSalt: "-test-salt", want: "TestA nested"},
		{name: "legacy", total: "4", index: "2", salt: "-test-salt", legacy: true, want: "TestA nested"},
		{name: "method filter", total: "4", index: "3", salt: "-test-salt", filter: "^TestB$", want: "TestB"},
		{name: "missing salt", total: "4", index: "0", wantErr: "sharding salt is required: set TEST_SHARD_SALT or use WithSalt"},
		{name: "invalid total", total: "0", index: "0", wantErr: "TEST_TOTAL_SHARDS must be positive: 0"},
		{name: "invalid index", total: "4", index: "4", wantErr: "TEST_SHARD_INDEX out of bounds: 4 must be in [0, 4)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestRunShardingProcess$", "-testify.m="+tc.filter)
			cmd.Env = append(os.Environ(),
				"TESTX_SHARD_PROCESS=1", "TEST_TOTAL_SHARDS="+tc.total, "TEST_SHARD_INDEX="+tc.index,
				"TEST_SHARD_SALT="+tc.salt, "TESTX_SHARD_DEFAULT_SALT="+tc.defaultSalt,
				"TESTX_SHARD_WANT="+tc.want, "TESTX_SHARD_LEGACY="+strconv.FormatBool(tc.legacy),
				"TESTX_SHARD_DISABLED="+strconv.FormatBool(tc.disabled))
			output, err := cmd.CombinedOutput()
			if tc.wantErr != "" {
				require.Error(t, err, "%s", output)
				require.Contains(t, string(output), tc.wantErr)
			} else {
				require.NoError(t, err, "%s", output)
			}
		})
	}
}
