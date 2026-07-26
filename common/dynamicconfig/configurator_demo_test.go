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
	e := NewConfiguratorEvaluator(AmbientConstraints{
		Environment:      "staging",
		AvailabilityZone: "us-west-2",
		ClusterName:      "active",
		ServiceName:      "matching",
		Custom:           map[string]any{"deployRing": 2},
	}, log.NewTestLogger())
	require.NoError(t, e.LoadFileFrom(demoConfigPath))

	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)

	t.Run("boolean logic over deployment dimensions", func(t *testing.T) {
		get := MatchingGetTasksBatchSize.Get(col)
		// env=staging and zone=us-west-2 matches the first override
		require.Equal(t, 250, get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	})

	t.Run("custom ambient dimension", func(t *testing.T) {
		// deployRing=2 > 1
		require.True(t, VisibilityAllowList.Get(col)("ns"))
	})

	t.Run("service targeting", func(t *testing.T) {
		// this evaluator says service=matching, so the history-only override must not match
		require.Equal(t, 9000, HistoryPersistenceMaxQPS.Get(col)())

		hist := NewConfiguratorEvaluator(AmbientConstraints{
			AvailabilityZone: "us-west-2", ServiceName: "history",
		}, log.NewTestLogger())
		require.NoError(t, hist.LoadFileFrom(demoConfigPath))
		histCol := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), hist)
		require.Equal(t, 18000, HistoryPersistenceMaxQPS.Get(histCol)())
	})

	t.Run("partition dimensions", func(t *testing.T) {
		get := MatchingMaxWaitForPollerBeforeFwd.Get(col)
		root := col.WithConstraints(map[string]any{"taskQueueIsRoot": true})
		child := col.WithConstraints(map[string]any{"taskQueueIsRoot": false})

		require.Equal(t, time.Second, MatchingMaxWaitForPollerBeforeFwd.Get(root)(
			"ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
		require.Equal(t, 200*time.Millisecond, MatchingMaxWaitForPollerBeforeFwd.Get(child)(
			"ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
		// with no partition dimensions attached at all, the default applies
		require.Equal(t, 200*time.Millisecond, get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	})

	t.Run("keys absent from the file are untouched", func(t *testing.T) {
		require.Equal(t,
			MatchingRPS.Get(NewNoopCollection())(),
			MatchingRPS.Get(col)())
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

	e := NewConfiguratorEvaluator(AmbientConstraints{}, log.NewTestLogger())
	require.NoError(t, e.LoadFileFrom(path))

	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
	cancelSub := e.Subscribe(col.EvaluatorKeysChanged)
	defer cancelSub()

	updates := make(chan int, 4)
	init, cancel := MatchingHistoryMaxPageSize.Subscribe(col)("ns", func(v int) { updates <- v })
	defer cancel()
	require.Equal(t, 11, init)

	stop := e.StartWatching(path, 20*time.Millisecond)
	defer stop()

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
	require.Error(t, e.LoadFileFrom(path))
	require.Equal(t, 22, MatchingHistoryMaxPageSize.Get(col)("ns"))
	require.Empty(t, updates, "a failed reload must not notify subscribers")
}
