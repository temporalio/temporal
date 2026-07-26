package dynamicconfig

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/log"
)

// These tests establish exactly what the Client-seam approach can and cannot do, as against
// the Evaluator approach in configurator_evaluator.go.

func newTestExprClient(t *testing.T, yaml string) *ConfiguratorClient {
	t.Helper()
	restoreExprSettings(t)
	c := NewConfiguratorClient(testExprAmbient, log.NewTestLogger())
	require.NoError(t, c.LoadFile([]byte(yaml)))
	return c
}

// The single unconstrained ConstrainedValue is matched at every precedence, because every
// precedence order ends in the empty Constraints.
func TestConfiguratorClient_MatchesAtEveryPrecedence(t *testing.T) {
	c := newTestExprClient(t, `
matching.historyMaxPageSize:      # namespace precedence
  defaultValue: 42
matching.getTasksBatchSize:       # task queue precedence
  defaultValue: 43
matching.numTaskqueueReadPartitions:  # task queue precedence, constrained default
  defaultValue: 44
history.persistenceMaxQPS:        # global precedence
  defaultValue: 45
`)
	col := NewCollection(c, log.NewTestLogger())

	require.Equal(t, 42, exprNamespaceInt.Get(col)("any-ns"))
	require.Equal(t, 43, exprGlobalInt.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 44, exprTQPartitions.Get(col)("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
	require.Equal(t, 45, HistoryPersistenceMaxQPS.Get(col)())
}

// Boolean logic over deployment-scoped dimensions works exactly as well as it does through
// the Evaluator: those dimensions are known to the process at load time.
func TestConfiguratorClient_AmbientDimensionsWork(t *testing.T) {
	const cfg = `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"zone" = "us-east-1"'
      matchResult: 2
    - matchString: '"env" = "staging" and ("zone" = "us-west-1" or "zone" = "us-west-2")'
      matchResult: 3
`
	col := NewCollection(newTestExprClient(t, cfg), log.NewTestLogger())
	require.Equal(t, 3, exprNamespaceInt.Get(col)("ns"))

	east := NewConfiguratorClient(AmbientConstraints{AvailabilityZone: "us-east-1"}, log.NewTestLogger())
	require.NoError(t, east.LoadFile([]byte(cfg)))
	require.Equal(t, 2, exprNamespaceInt.Get(NewCollection(east, log.NewTestLogger()))("ns"))
}

// The structural limit. GetValue is handed a Key and nothing else, so an expression over a
// per-request dimension has nothing to match against and silently falls to the default.
func TestConfiguratorClient_CannotSeePerRequestDimensions(t *testing.T) {
	col := NewCollection(newTestExprClient(t, `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"namespace" = "canary"'
      matchResult: 99
`), log.NewTestLogger())

	// The override can never fire: the client is not told which namespace is asking.
	require.Equal(t, 1, exprNamespaceInt.Get(col)("canary"))
	require.Equal(t, 1, exprNamespaceInt.Get(col)("anything-else"))

	// The Evaluator, hooked in where the constraints are known, gets this right.
	ev := newTestEvaluator(t, `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"namespace" = "canary"'
      matchResult: 99
`)
	evCol := NewCollectionWithEvaluator(NewNoopClient(), log.NewTestLogger(), ev)
	require.Equal(t, 99, exprNamespaceInt.Get(evCol)("canary"))
	require.Equal(t, 1, exprNamespaceInt.Get(evCol)("anything-else"))
}

// Reads through the client cost what they cost today: evaluation happened at load, and
// GetValue returns a stable slice so Collection's conversion cache still hits.
func BenchmarkConfiguratorClient(b *testing.B) {
	c := NewConfiguratorClient(AmbientConstraints{Environment: "staging"}, log.NewNoopLogger())
	if err := c.LoadFile([]byte(
		"matching.getTasksBatchSize:\n  defaultValue: 100\n  overrides:\n" +
			"    - matchString: '\"env\" = \"staging\"'\n      matchResult: 250\n")); err != nil {
		b.Fatal(err)
	}
	col := NewCollection(c, log.NewNoopLogger())
	get := MatchingGetTasksBatchSize.Get(col)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = get("ns", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	}
}
