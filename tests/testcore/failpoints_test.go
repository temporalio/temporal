package testcore

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/testing/serverbuggify"
	"go.temporal.io/testx/failpoint"
	"gopkg.in/yaml.v3"
)

// recordingTB records whether the test failed instead of failing the real test.
type recordingTB struct {
	testing.TB
	failed bool
}

func (r *recordingTB) Helper() {}

func (r *recordingTB) Errorf(string, ...any) { r.failed = true }

var requireFiredKey = failpoint.NewKey[error]("testcore.requireFired")

func TestRequireFired(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configure  func(fp *failpoint.Failpoint[error])
		executions int
		wantFailed bool
	}{
		{name: "never executed", wantFailed: true},
		{name: "executed while off", executions: 2, wantFailed: true},
		{
			name:       "fired",
			configure:  func(fp *failpoint.Failpoint[error]) { fp.On() },
			executions: 1,
		},
		{
			name: "fired before the trigger was set again",
			configure: func(fp *failpoint.Failpoint[error]) {
				fp.On()
				_, _ = fp.Execute()
				require.NoError(t, fp.SetTrigger(failpoint.Trigger{SkipFirst: 1}))
			},
			executions: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp := failpoint.NewFailpoint(requireFiredKey)
			if tc.configure != nil {
				tc.configure(fp)
			}
			for range tc.executions {
				_, _ = fp.Execute()
			}
			recorder := &recordingTB{TB: t}
			requireFired(recorder, requireFiredKey, fp)
			require.Equal(t, tc.wantFailed, recorder.failed)
		})
	}
}

// WithBuggify gives the test a dedicated cluster whose servers have the failpoints.buggify block.
// The YAML list of the guide is the standard list that StandardBuggifyKeys returns, and the list
// is valid, never fails a persistence, response or health key and fails the gRPC and HTTP calls
// between services with Unavailable.
func TestStandardBuggifyKeys(t *testing.T) {
	t.Parallel()

	t.Run("the guide has the same list", func(t *testing.T) {
		t.Parallel()
		guide, err := os.ReadFile("../../docs/development/failpoints.md")
		require.NoError(t, err)
		_, after, found := strings.Cut(string(guide), "<!-- standardBuggifyKeys")
		require.True(t, found, "the guide marks the standard list")
		_, after, found = strings.Cut(after, "```yaml\n")
		require.True(t, found)
		block, _, found := strings.Cut(after, "```")
		require.True(t, found)

		var cfg config.Config
		require.NoError(t, yaml.Unmarshal([]byte(block), &cfg))
		buggify, err := cfg.Failpoints.ReadBuggify()
		require.NoError(t, err)
		require.Equal(t, StandardBuggifyKeys(), buggify.Keys)
	})

	t.Run("selects the keys that BUGGIFY applies to", func(t *testing.T) {
		t.Parallel()
		static, err := serverbuggify.ReadStatic(config.FailpointsBuggify{Seed: 1, Keys: StandardBuggifyKeys()})
		require.NoError(t, err)

		// Which keys are activated depends on the seed, so look for each of them over many seeds.
		activatedFor := func(name string) bool {
			for seed := uint64(1); seed <= 200; seed++ {
				c := static.Config
				c.Seed = seed
				if c.Activated(name) {
					return true
				}
			}
			return false
		}
		for name, want := range map[string]bool{
			"grpc:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": true,
			"http:POST":              true,
			"matching.skipSyncMatch": true,
			serverbuggify.SettingPrefix + "history.transferTaskBatchSize":                                true,
			"grpc:/grpc.health.v1.Health/Check":                                                          false,
			"grpc-response:/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution": false,
			"http-response:POST": false,
			"persistence:ExecutionStore.UpdateWorkflowExecution": false,
			"grpc-any:request": false,
		} {
			require.Equal(t, want, activatedFor(name), name)
		}
	})
}

func TestWithBuggify(t *testing.T) {
	t.Parallel()

	block := config.FailpointsBuggify{Seed: 42, Keys: StandardBuggifyKeys(), Trace: true}
	var options testOptions
	WithBuggify(block)(&options)

	require.True(t, options.dedicatedCluster)
	require.Equal(t, block, ApplyTestClusterOptions(options.clusterOptions).Buggify)
	require.Equal(t, config.FailpointsBuggify{}, ApplyTestClusterOptions(nil).Buggify)
}

// WithClusterBuggify sets the block for the cluster options of a suite, such as an XDC suite.
func TestWithClusterBuggify(t *testing.T) {
	t.Parallel()

	block := config.FailpointsBuggify{Seed: 42}
	require.Equal(t, block, ApplyTestClusterOptions([]TestClusterOption{WithClusterBuggify(block)}).Buggify)
}
