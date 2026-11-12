package parallelsuite

import (
	"os"
	"os/exec"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	sharedsuite "go.temporal.io/testx/parallelsuite"
	"go.temporal.io/testx/testshard"
)

type explicitShardSuite struct{ Suite[*explicitShardSuite] }

func (s *explicitShardSuite) TestRuns(ran chan bool) { s.Snd(ran, true) }

func TestExplicitShardProcess(t *testing.T) {
	if os.Getenv("TESTX_EXPLICIT_SHARD_PROCESS") == "" {
		return
	}
	ran := make(chan bool, 1)
	t.Cleanup(func() { require.Len(t, ran, 1) })
	args := []any{sharedsuite.WithShard(testshard.Shard{}), ran}
	t.Run("suite", func(t *testing.T) {
		if os.Getenv("TESTX_EXPLICIT_SHARD_LEGACY") == "true" {
			RunLegacySequential(t, &explicitShardSuite{}, args...) //nolint:staticcheck // SA1019: explicit shards must work with the legacy wrapper.
		} else {
			Run(t, &explicitShardSuite{}, args...)
		}
	})
}

func TestExplicitShardOverridesServerDefault(t *testing.T) {
	t.Parallel()
	for _, legacy := range []bool{false, true} {
		t.Run(strconv.FormatBool(legacy), func(t *testing.T) {
			t.Parallel()
			executable, err := os.Executable()
			require.NoError(t, err)
			cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestExplicitShardProcess$")
			cmd.Env = append(os.Environ(), "TESTX_EXPLICIT_SHARD_PROCESS=1",
				"TESTX_EXPLICIT_SHARD_LEGACY="+strconv.FormatBool(legacy),
				"TEST_TOTAL_SHARDS=invalid", "TEST_SHARD_INDEX=invalid")
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, "%s", output)
		})
	}
}
