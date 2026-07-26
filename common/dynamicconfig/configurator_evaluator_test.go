package dynamicconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
)

var (
	testExprAmbient = AmbientConstraints{
		Environment:      "staging",
		AvailabilityZone: "us-west-2",
		ClusterName:      "active",
		ServiceName:      "matching",
		Custom:           map[string]any{"deployRing": 2},
	}

	// Real settings, chosen to cover the precedences and value types that matter:
	//   global int, namespace int, task queue int with a constrained default, global duration.
	exprGlobalInt    = MatchingGetTasksBatchSize          // task queue precedence, int
	exprNamespaceInt = MatchingHistoryMaxPageSize         // namespace precedence, int
	exprTQPartitions = MatchingNumTaskqueueReadPartitions // task queue precedence, int, constrained default
	exprDuration     = MatchingLongPollExpirationInterval // task queue precedence, duration
	exprNsBool       = VisibilityAllowList                // namespace precedence, bool
)

// restoreExprSettings re-registers the settings these tests use.
//
// Several suites in this package call ResetRegistryForTest, which empties the global
// registry for the whole test binary. Settings register themselves from package init, so
// once wiped they never come back, and anything that consults the registry (value
// validation at load, subscriber dispatch) silently becomes a no-op. Under -count=2 or with
// -shuffle that turns into confusing failures, so restore explicitly rather than depending
// on test order.
func restoreExprSettings(t *testing.T) {
	t.Helper()
	for _, s := range []GenericSetting{
		exprGlobalInt, exprNamespaceInt, exprTQPartitions, exprDuration, exprNsBool,
		HistoryPersistenceMaxQPS, MatchingRPS,
	} {
		if queryRegistry(s.Key()) == nil {
			// queryRegistry latches `queried`, which makes register panic.
			globalRegistry.queried.Store(false)
			register(s)
		}
	}
}

func newTestEvaluator(t *testing.T, yaml string) *ConfiguratorEvaluator {
	t.Helper()
	restoreExprSettings(t)
	e := NewConfiguratorEvaluator(testExprAmbient, log.NewTestLogger())
	require.NoError(t, e.LoadFile([]byte(yaml)))
	return e
}

func TestConfiguratorEvaluator_NoConfigIsInert(t *testing.T) {
	restoreExprSettings(t)
	// An evaluator with nothing loaded must behave exactly like having no evaluator.
	e := NewConfiguratorEvaluator(testExprAmbient, log.NewTestLogger())
	require.False(t, e.Has(exprGlobalInt.Key()))
	require.Nil(t, e.Eval(exprGlobalInt.Key(), Constraints{}, nil))

	cln := StaticClient{exprNamespaceInt.Key(): 111}
	col := NewCollectionWithEvaluator(cln, log.NewTestLogger(), e)
	require.Equal(t, 111, exprNamespaceInt.Get(col)("some-ns"))
}

func TestConfiguratorEvaluator_DefaultAndOverrides(t *testing.T) {
	e := newTestEvaluator(t, `
matching.historyMaxPageSize:
  defaultValue: 100
  overrides:
    - matchString: '"namespace" = "canary"'
      matchResult: 5
    - matchString: '"env" = "staging" and ("zone" = "us-west-1" or "zone" = "us-west-2")'
      matchResult: 42
`)
	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
	get := exprNamespaceInt.Get(col)

	// first matching override wins, in file order
	require.Equal(t, 5, get("canary"))
	// the boolean expression matches on ambient dimensions alone
	require.Equal(t, 42, get("other-ns"))
}

func TestConfiguratorEvaluator_AmbientDimensionsAreMatchable(t *testing.T) {
	// The point of the prototype: dimensions that Constraints cannot express at all.
	const cfg = `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"zone" = "us-east-1"'
      matchResult: 2
    - matchString: '"deployRing" > 1 and "service" = "matching"'
      matchResult: 3
`
	e := newTestEvaluator(t, cfg)
	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
	require.Equal(t, 3, exprNamespaceInt.Get(col)("ns"))

	// Same file, different deployment: no code change, different value.
	other := NewConfiguratorEvaluator(AmbientConstraints{AvailabilityZone: "us-east-1"}, log.NewTestLogger())
	require.NoError(t, other.LoadFile([]byte(cfg)))
	otherCol := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), other)
	require.Equal(t, 2, exprNamespaceInt.Get(otherCol)("ns"))
}

func TestConfiguratorEvaluator_PrecedenceConstraintsAreExposed(t *testing.T) {
	e := newTestEvaluator(t, `
matching.getTasksBatchSize:
  defaultValue: 10
  overrides:
    - matchString: '"namespace" = "ns1" and "taskQueueName" = "tq1" and "taskQueueType" = "Activity"'
      matchResult: 20
    - matchString: '"taskQueueName" = "tq1"'
      matchResult: 30
`)
	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
	get := exprGlobalInt.Get(col)

	require.Equal(t, 20, get("ns1", "tq1", enumspb.TASK_QUEUE_TYPE_ACTIVITY))
	// taskQueueType differs, so the first override no longer matches; the second does
	require.Equal(t, 30, get("ns1", "tq1", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 10, get("ns1", "tq2", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

func TestConfiguratorEvaluator_DurationAndBoolConversion(t *testing.T) {
	e := newTestEvaluator(t, `
matching.longPollExpirationInterval:
  defaultValue: 1m
  overrides:
    - matchString: '"namespace" = "slow"'
      matchResult: 30s
system.visibilityAllowList:
  defaultValue: false
  overrides:
    - matchString: '"env" = "staging"'
      matchResult: true
`)
	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)

	require.Equal(t, time.Minute, exprDuration.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 30*time.Second, exprDuration.Get(col)("slow", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.True(t, exprNsBool.Get(col)("ns"))
}

func TestConfiguratorEvaluator_BeatsClientAndConstrainedDefault(t *testing.T) {
	cln := StaticClient{
		exprNamespaceInt.Key(): 7,
		exprTQPartitions.Key(): 7,
	}

	e := newTestEvaluator(t, `
matching.historyMaxPageSize:
  defaultValue: 99
matching.numTaskqueueReadPartitions:
  defaultValue: 16
`)
	col := NewCollectionWithEvaluator(cln, log.NewTestLogger(), e)

	require.Equal(t, 99, exprNamespaceInt.Get(col)("ns"))
	require.Equal(t, 16, exprTQPartitions.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

func TestConfiguratorEvaluator_UnconfiguredKeysFallThrough(t *testing.T) {
	cln := StaticClient{exprNamespaceInt.Key(): 7}
	e := newTestEvaluator(t, `
matching.getTasksBatchSize:
  defaultValue: 1
`)
	col := NewCollectionWithEvaluator(cln, log.NewTestLogger(), e)

	// configured in the expression file
	require.Equal(t, 1, exprGlobalInt.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	// not configured there: the client wins
	require.Equal(t, 7, exprNamespaceInt.Get(col)("ns"))
	// configured nowhere: the compiled-in default
	require.Equal(t, exprDuration.Get(NewNoopCollection())("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		exprDuration.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

func TestConfiguratorEvaluator_LoadErrors(t *testing.T) {
	restoreExprSettings(t)
	e := NewConfiguratorEvaluator(testExprAmbient, log.NewTestLogger())

	t.Run("malformed expression", func(t *testing.T) {
		err := e.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: 'this is not a valid expression'
      matchResult: 2
`))
		require.Error(t, err)
	})

	t.Run("wrong value type", func(t *testing.T) {
		err := e.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: "not an int"
`))
		require.ErrorContains(t, err, "matching.historymaxpagesize")
	})

	t.Run("wrong override value type", func(t *testing.T) {
		err := e.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"env" = "staging"'
      matchResult: "not an int"
`))
		require.ErrorContains(t, err, "override 0")
	})

	t.Run("failed load leaves previous config in place", func(t *testing.T) {
		require.NoError(t, e.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 5\n")))
		col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
		require.Equal(t, 5, exprNamespaceInt.Get(col)("ns"))

		require.Error(t, e.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: bogus\n")))
		require.Equal(t, 5, exprNamespaceInt.Get(col)("ns"))
	})
}

func TestConfiguratorEvaluator_WithConstraints(t *testing.T) {
	// Layer 2: a dimension that has no field in Constraints and no precedence order.
	e := newTestEvaluator(t, `
matching.numTaskqueueReadPartitions:
  defaultValue: 4
  overrides:
    - matchString: '"taskQueuePartitionID" > 0 and "zone" = "us-west-2"'
      matchResult: 16
`)
	root := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)

	require.Equal(t, 4, exprTQPartitions.Get(root)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))

	p0 := root.WithConstraints(map[string]any{"taskQueuePartitionID": 0})
	p3 := root.WithConstraints(map[string]any{"taskQueuePartitionID": 3})
	require.Equal(t, 4, exprTQPartitions.Get(p0)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 16, exprTQPartitions.Get(p3)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))

	// derived views compose, most recent wins
	require.Equal(t, 4, exprTQPartitions.Get(p3.WithConstraints(map[string]any{"taskQueuePartitionID": 0}))(
		"ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

func TestConfiguratorEvaluator_SubscriptionsSeeReloads(t *testing.T) {
	e := newTestEvaluator(t, "matching.historyMaxPageSize:\n  defaultValue: 1\n")
	col := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), e)
	cancelSub := e.Subscribe(col.EvaluatorKeysChanged)
	defer cancelSub()

	updates := make(chan int, 4)
	init, cancel := exprNamespaceInt.Subscribe(col)("ns", func(v int) { updates <- v })
	defer cancel()
	require.Equal(t, 1, init)

	require.NoError(t, e.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 2\n")))
	require.Equal(t, 2, <-updates)

	// A reload that does not change this key must not notify.
	require.NoError(t, e.LoadFile([]byte(
		"matching.historyMaxPageSize:\n  defaultValue: 2\nmatching.getTasksBatchSize:\n  defaultValue: 9\n")))
	select {
	case v := <-updates:
		t.Fatalf("unexpected update to %v", v)
	case <-time.After(100 * time.Millisecond):
	}

	// Removing the key from the expression config falls back to the client/default.
	require.NoError(t, e.LoadFile([]byte("matching.getTasksBatchSize:\n  defaultValue: 9\n")))
	require.Equal(t, exprNamespaceInt.Get(NewNoopCollection())("ns"), <-updates)
}

func TestConfiguratorEvaluator_ChangedKeys(t *testing.T) {
	restoreExprSettings(t)
	e := NewConfiguratorEvaluator(testExprAmbient, log.NewTestLogger())
	var got [][]Key
	cancelSub := e.Subscribe(func(keys []Key) { got = append(got, keys) })
	defer cancelSub()

	require.NoError(t, e.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 1\n")))
	require.Equal(t, [][]Key{{exprNamespaceInt.Key()}}, got)

	// unchanged
	require.NoError(t, e.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 1\n")))
	require.Len(t, got, 1)

	// changed only by an added override
	require.NoError(t, e.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"env" = "prod"'
      matchResult: 2
`)))
	require.Equal(t, []Key{exprNamespaceInt.Key()}, got[1])
}
