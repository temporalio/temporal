package dynamicconfig

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
)

const demoConfigPath = "../../config/dynamicconfig/expressions-demo.yaml"

// TestDemoExpressionConfig loads the shipped demo file and asserts it resolves the way its
// comments claim, so the documentation cannot drift from the behaviour.
func TestDemoExpressionConfig(t *testing.T) {
	restoreExprSettings(t)

	load := func(t *testing.T, ambient AmbientConstraints) *Collection {
		t.Helper()
		c := NewConfiguratorClient(ambient, nil, log.NewTestLogger())
		require.NoError(t, c.LoadFileFrom(demoConfigPath))
		return NewCollection(c, log.NewTestLogger())
	}

	matching := load(t, AmbientConstraints{
		Environment:      "staging",
		AvailabilityZone: "us-west-2",
		ClusterName:      "active",
		ServiceName:      "matching",
		Custom:           map[string]any{"deployRing": 2},
	})

	t.Run("boolean logic over deployment dimensions", func(t *testing.T) {
		// env=staging and zone=us-west-2 matches the override
		require.Equal(t, 250,
			MatchingGetTasksBatchSize.Get(matching)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	})

	t.Run("custom ambient dimension", func(t *testing.T) {
		require.True(t, VisibilityAllowList.Get(matching)("ns")) // deployRing=2 > 1
	})

	t.Run("service targeting", func(t *testing.T) {
		// this process says service=matching, so the history-only override must not match
		require.Equal(t, 9000, HistoryPersistenceMaxQPS.Get(matching)())

		history := load(t, AmbientConstraints{AvailabilityZone: "us-west-2", ServiceName: "history"})
		require.Equal(t, 18000, HistoryPersistenceMaxQPS.Get(history)())
	})

	t.Run("cluster targeting", func(t *testing.T) {
		require.Equal(t, time.Minute,
			MatchingLongPollExpirationInterval.Get(matching)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))

		standby := load(t, AmbientConstraints{ClusterName: "standby"})
		require.Equal(t, 20*time.Second,
			MatchingLongPollExpirationInterval.Get(standby)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	})

	t.Run("keys absent from the file are untouched", func(t *testing.T) {
		require.Equal(t, MatchingRPS.Get(NewNoopCollection())(), MatchingRPS.Get(matching)())
	})

	t.Run("per-caller dimensions", func(t *testing.T) {
		history := load(t, AmbientConstraints{AvailabilityZone: "us-west-2", ServiceName: "history"})
		get := EnableEagerWorkflowStart.GetC(history)

		// an SDK version the config singles out
		require.False(t, get(ConstraintsWithNS("ns").
			With(CKSDKName, "temporal-go").With(CKSDKMajor, 1).With(CKSDKMinor, 27)))
		// a newer one is unaffected
		require.True(t, get(ConstraintsWithNS("ns").
			With(CKSDKName, "temporal-go").With(CKSDKMajor, 1).With(CKSDKMinor, 28)))
		// and a different SDK entirely
		require.True(t, get(ConstraintsWithNS("ns").
			With(CKSDKName, "temporal-java").With(CKSDKMajor, 1).With(CKSDKMinor, 27)))

		// the namespace override still applies, and needs no SDK information
		require.False(t, get(ConstraintsWithNS("canary")))
		// a caller supplying nothing gets the default, since neither override can match
		require.True(t, get(nil))
	})
}

// TestDemoExpressionConfigReload covers the watch path against a real file on disk.
func TestDemoExpressionConfigReload(t *testing.T) {
	restoreExprSettings(t)

	path := filepath.Join(t.TempDir(), "expr.yaml")
	// The watcher keys off modification time, which on some filesystems has coarse
	// granularity, so stamp each write explicitly rather than sleeping between them.
	mtime := time.Now().Add(-time.Hour)
	write := func(contents string) {
		require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
		mtime = mtime.Add(time.Minute)
		require.NoError(t, os.Chtimes(path, mtime, mtime))
	}
	write("matching.historyMaxPageSize:\n  defaultValue: 11\n")

	c := NewConfiguratorClient(AmbientConstraints{}, nil, log.NewTestLogger())
	t.Cleanup(c.Stop)
	require.NoError(t, c.LoadFileFrom(path))

	col := NewCollection(c, log.NewTestLogger())
	col.Start()
	t.Cleanup(col.Stop)

	updates := make(chan int, 4)
	init, cancel := MatchingHistoryMaxPageSize.Subscribe(col)("ns", func(v int) { updates <- v })
	t.Cleanup(cancel)
	require.Equal(t, 11, init)

	stop := c.StartWatching(path, 20*time.Millisecond)
	t.Cleanup(stop)

	write("matching.historyMaxPageSize:\n  defaultValue: 22\n")
	select {
	case v := <-updates:
		require.Equal(t, 22, v)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for reload")
	}
	require.Equal(t, 22, MatchingHistoryMaxPageSize.Get(col)("ns"))

	// A file that fails to parse must not clobber the running config. Drive this load
	// directly rather than through the watcher so there is nothing to race with.
	stop()
	write("matching.historyMaxPageSize:\n  defaultValue: not-an-int\n")
	require.Error(t, c.LoadFileFrom(path))
	require.Equal(t, 22, MatchingHistoryMaxPageSize.Get(col)("ns"))
	require.Empty(t, updates, "a failed reload must not notify subscribers")
}
