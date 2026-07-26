package dynamicconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
)

var (
	testExprAmbient = AmbientConstraints{
		Environment:      "staging",
		AvailabilityZone: "us-west-2",
		ClusterName:      "active",
		ServiceName:      "matching",
		Custom:           map[string]any{"deployRing": 2},
	}

	// Real settings, chosen to cover the precedences and value types that matter.
	exprTaskQueueInt = MatchingGetTasksBatchSize          // task queue precedence, int
	exprNamespaceInt = MatchingHistoryMaxPageSize         // namespace precedence, int
	exprTQPartitions = MatchingNumTaskqueueReadPartitions // task queue precedence, constrained default
	exprDuration     = MatchingLongPollExpirationInterval // task queue precedence, duration
	exprNsBool       = VisibilityAllowList                // namespace precedence, bool
)

// restoreExprSettings re-registers the settings these tests use.
//
// Several suites in this package call ResetRegistryForTest, which empties the global registry
// for the whole test binary. Settings register themselves from package init, so once wiped
// they never come back, and anything that consults the registry (value validation at load)
// silently becomes a no-op. Under -count=2 or with -shuffle that turns into confusing
// failures, so restore explicitly rather than depending on test order.
func restoreExprSettings(t *testing.T) {
	t.Helper()
	for _, s := range []GenericSetting{
		exprTaskQueueInt, exprNamespaceInt, exprTQPartitions, exprDuration, exprNsBool,
		EnableEagerWorkflowStart,
		HistoryPersistenceMaxQPS, MatchingRPS,
	} {
		if queryRegistry(s.Key()) == nil {
			// queryRegistry latches `queried`, which makes register panic.
			globalRegistry.queried.Store(false)
			register(s)
		}
	}
}

func newTestExprClient(t *testing.T, inner Client, yaml string) *ConfiguratorClient {
	t.Helper()
	restoreExprSettings(t)
	c := NewConfiguratorClient(testExprAmbient, inner, log.NewTestLogger())
	t.Cleanup(c.Stop)
	require.NoError(t, c.LoadFile([]byte(yaml)))
	return c
}

// The single unconstrained ConstrainedValue is matched at every precedence, because every
// precedence order ends in the empty Constraints. This is the whole reason the Client seam
// works without enumerating anything.
func TestConfiguratorClient_MatchesAtEveryPrecedence(t *testing.T) {
	c := newTestExprClient(t, nil, `
matching.historyMaxPageSize:         # namespace precedence
  defaultValue: 42
matching.getTasksBatchSize:          # task queue precedence
  defaultValue: 43
matching.numTaskqueueReadPartitions: # task queue precedence, constrained default
  defaultValue: 44
history.persistenceMaxQPS:           # global precedence
  defaultValue: 45
matching.longPollExpirationInterval: # duration, via string conversion
  defaultValue: 30s
system.visibilityAllowList:          # bool
  defaultValue: true
`)
	col := NewCollection(c, log.NewTestLogger())

	require.Equal(t, 42, exprNamespaceInt.Get(col)("any-ns"))
	require.Equal(t, 43, exprTaskQueueInt.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 44, exprTQPartitions.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 45, HistoryPersistenceMaxQPS.Get(col)())
	require.Equal(t, 30*time.Second, exprDuration.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.True(t, exprNsBool.Get(col)("ns"))
}

// Boolean logic over deployment-scoped dimensions: the point of the exercise.
func TestConfiguratorClient_AmbientDimensions(t *testing.T) {
	const cfg = `
constraintKeys: [deployRing]
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"zone" = "us-east-1"'
      matchResult: 2
    - matchString: '"env" = "staging" and ("zone" = "us-west-1" or "zone" = "us-west-2")'
      matchResult: 3
    - matchString: '"deployRing" > 1'
      matchResult: 4
`
	col := NewCollection(newTestExprClient(t, nil, cfg), log.NewTestLogger())
	// first match in file order wins, so 3 rather than 4
	require.Equal(t, 3, exprNamespaceInt.Get(col)("ns"))

	// Same file, different deployment: no code change, different value.
	east := NewConfiguratorClient(AmbientConstraints{AvailabilityZone: "us-east-1"}, nil, log.NewTestLogger())
	require.NoError(t, east.LoadFile([]byte(cfg)))
	require.Equal(t, 2, exprNamespaceInt.Get(NewCollection(east, log.NewTestLogger()))("ns"))

	// And a deployment matching nothing gets the default.
	none := NewConfiguratorClient(AmbientConstraints{AvailabilityZone: "eu-west-1"}, nil, log.NewTestLogger())
	require.NoError(t, none.LoadFile([]byte(cfg)))
	require.Equal(t, 1, exprNamespaceInt.Get(NewCollection(none, log.NewTestLogger()))("ns"))
}

// The structural limit of this seam. GetValue is handed a Key and nothing else, so an
// expression over a per-request dimension has nothing to match against.
func TestConfiguratorClient_CannotSeePerRequestDimensions(t *testing.T) {
	col := NewCollection(newTestExprClient(t, nil, `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"namespace" = "canary"'
      matchResult: 99
`), log.NewTestLogger())

	// The override can never fire: the client is not told which namespace is asking.
	require.Equal(t, 1, exprNamespaceInt.Get(col)("canary"))
	require.Equal(t, 1, exprNamespaceInt.Get(col)("anything-else"))
}

// Layering: a key in the expression file is served from there; everything else is delegated.
func TestConfiguratorClient_LayersOverInnerClient(t *testing.T) {
	inner := StaticClient{
		exprNamespaceInt.Key(): 7,
		exprTaskQueueInt.Key(): 8,
	}
	col := NewCollection(newTestExprClient(t, inner, `
matching.historyMaxPageSize:
  defaultValue: 99
`), log.NewTestLogger())

	// configured by expression: the inner client is ignored for this key
	require.Equal(t, 99, exprNamespaceInt.Get(col)("ns"))
	// not configured by expression: delegated
	require.Equal(t, 8, exprTaskQueueInt.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	// configured nowhere: compiled-in default
	require.Equal(t,
		MatchingRPS.Get(NewNoopCollection())(),
		MatchingRPS.Get(col)())
}

// A key configured by expression is served *entirely* from the expression file: a
// constrained value for that key in the inner client does not win, even though its
// constraints are more specific.
func TestConfiguratorClient_ExpressionKeyIgnoresInnerConstraints(t *testing.T) {
	inner := StaticClient{
		exprNamespaceInt.Key(): []ConstrainedValue{
			{Constraints: Constraints{Namespace: "canary"}, Value: 7},
			{Value: 8},
		},
	}
	col := NewCollection(newTestExprClient(t, inner, `
matching.historyMaxPageSize:
  defaultValue: 99
`), log.NewTestLogger())

	require.Equal(t, 99, exprNamespaceInt.Get(col)("canary"))
	require.Equal(t, 99, exprNamespaceInt.Get(col)("other"))
}

// An unconstrained expression value loses to a more specific constrained *default*. This is
// not new behaviour: an unconstrained value in the dynamic config file behaves identically.
// It is pinned here because it is surprising.
func TestConfiguratorClient_ConstrainedDefaultsStillWin(t *testing.T) {
	col := NewCollection(newTestExprClient(t, nil, `
matching.numTaskqueueReadPartitions:
  defaultValue: 16
`), log.NewTestLogger())
	get := exprTQPartitions.Get(col)

	// ordinary task queue: the expression value applies
	require.Equal(t, 16, get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))

	// the per-namespace worker task queue has a constrained default of 1, which is more
	// specific than our unconstrained value and therefore still wins
	require.Equal(t, 1, get("ns", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW))

	// exactly what the file based client does with an unconstrained value
	fileCol := NewCollection(StaticClient{exprTQPartitions.Key(): 16}, log.NewTestLogger())
	require.Equal(t, 1,
		exprTQPartitions.Get(fileCol)("ns", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW))
}

func TestConfiguratorClient_LoadErrors(t *testing.T) {
	restoreExprSettings(t)
	c := NewConfiguratorClient(testExprAmbient, nil, log.NewTestLogger())

	t.Run("malformed expression", func(t *testing.T) {
		require.Error(t, c.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: 'this is not a valid expression'
      matchResult: 2
`)))
	})

	t.Run("wrong value type", func(t *testing.T) {
		require.ErrorContains(t, c.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: "not an int"
`)), "matching.historymaxpagesize")
	})

	t.Run("wrong override value type", func(t *testing.T) {
		require.ErrorContains(t, c.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"env" = "staging"'
      matchResult: "not an int"
`)), "override 0")
	})

	t.Run("failed load leaves previous config in place", func(t *testing.T) {
		require.NoError(t, c.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 5\n")))
		col := NewCollection(c, log.NewTestLogger())
		require.Equal(t, 5, exprNamespaceInt.Get(col)("ns"))

		require.Error(t, c.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: bogus\n")))
		require.Equal(t, 5, exprNamespaceInt.Get(col)("ns"))
	})
}

func TestConfiguratorClient_Subscriptions(t *testing.T) {
	restoreExprSettings(t)
	inner := NewMemoryClient()
	c := NewConfiguratorClient(testExprAmbient, inner, log.NewTestLogger())
	t.Cleanup(c.Stop)
	require.NoError(t, c.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 1\n")))

	col := NewCollection(c, log.NewTestLogger())
	col.Start()
	t.Cleanup(col.Stop)

	updates := make(chan int, 4)
	init, cancel := exprNamespaceInt.Subscribe(col)("ns", func(v int) { updates <- v })
	t.Cleanup(cancel)
	require.Equal(t, 1, init)

	t.Run("expression reload notifies", func(t *testing.T) {
		require.NoError(t, c.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 2\n")))
		require.Equal(t, 2, <-updates)
	})

	t.Run("a reload that changes nothing does not notify", func(t *testing.T) {
		require.NoError(t, c.LoadFile([]byte("matching.historyMaxPageSize:\n  defaultValue: 2\n")))
		select {
		case v := <-updates:
			t.Fatalf("unexpected update to %v", v)
		case <-time.After(100 * time.Millisecond):
		}
	})

	// Note: the override below must outlive its subtest, so it is cleaned up on the parent.
	parent := t

	t.Run("inner updates to an overridden key do not leak through", func(t *testing.T) {
		parent.Cleanup(inner.OverrideValue(exprNamespaceInt.Key(), 555))
		select {
		case v := <-updates:
			t.Fatalf("inner client update leaked through as %v", v)
		case <-time.After(100 * time.Millisecond):
		}
		require.Equal(t, 2, exprNamespaceInt.Get(col)("ns"))
	})

	t.Run("dropping a key falls back to the inner client", func(t *testing.T) {
		require.NoError(t, c.LoadFile([]byte("matching.getTasksBatchSize:\n  defaultValue: 9\n")))
		require.Equal(t, 555, <-updates)
	})
}

// Reads through the client cost what they cost today: evaluation happened at load, and
// GetValue returns a stable slice so Collection's conversion cache still hits.
func BenchmarkConfiguratorClient(b *testing.B) {
	inner := StaticClient{
		MatchingGetTasksBatchSize.Key(): []ConstrainedValue{
			{Value: 100},
			{Constraints: Constraints{Namespace: "ns-7"}, Value: 25},
		},
	}

	b.Run("today's path", func(b *testing.B) {
		benchGetTasksBatchSize(b, NewCollection(inner, log.NewNoopLogger()))
	})

	b.Run("expression-backed key", func(b *testing.B) {
		c := NewConfiguratorClient(AmbientConstraints{Environment: "staging", AvailabilityZone: "us-west-2"},
			inner, log.NewNoopLogger())
		if err := c.LoadFile([]byte(
			"matching.getTasksBatchSize:\n  defaultValue: 100\n  overrides:\n" +
				"    - matchString: '\"env\" = \"staging\" and (\"zone\" = \"us-west-1\" or \"zone\" = \"us-west-2\")'\n" +
				"      matchResult: 25\n")); err != nil {
			b.Fatal(err)
		}
		benchGetTasksBatchSize(b, NewCollection(c, log.NewNoopLogger()))
	})

	b.Run("delegated key", func(b *testing.B) {
		c := NewConfiguratorClient(AmbientConstraints{}, inner, log.NewNoopLogger())
		if err := c.LoadFile([]byte("history.rps:\n  defaultValue: 1\n")); err != nil {
			b.Fatal(err)
		}
		benchGetTasksBatchSize(b, NewCollection(c, log.NewNoopLogger()))
	})
}

func benchGetTasksBatchSize(b *testing.B, col *Collection) {
	b.ReportAllocs()
	get := MatchingGetTasksBatchSize.Get(col)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = get("ns-7", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
}
