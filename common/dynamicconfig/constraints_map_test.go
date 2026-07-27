package dynamicconfig

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
)

func TestConstraintsMap_Builders(t *testing.T) {
	c := ConstraintsWithNS("ns1").With("sdkName", "temporal-go").With("sdkMajor", 2)
	require.Equal(t, ConstraintsMap{
		CKNamespace: "ns1", "sdkName": "temporal-go", "sdkMajor": 2,
	}, c)

	// With mutates in place on a non-nil map, so the statement form works.
	c.With("extra", true)
	require.Equal(t, true, c["extra"])

	// ...but a nil map has to allocate, so the result must be used.
	var nilMap ConstraintsMap
	require.Equal(t, ConstraintsMap{"k": 1}, nilMap.With("k", 1))
	require.Nil(t, nilMap)

	// Get satisfies the expression library's Lookup interface.
	v, ok := c.Get(CKNamespace)
	require.True(t, ok)
	require.Equal(t, "ns1", v)
	_, ok = c.Get("absent")
	require.False(t, ok)
}

func TestConstraintsFromContext(t *testing.T) {
	ctx := headers.SetVersionsForTests(t.Context(), "1.28.3", "temporal-go", "", "")
	c := ConstraintsFromContext(ctx)

	require.Equal(t, "temporal-go", c[CKSDKName])
	require.Equal(t, "1.28.3", c[CKSDKVersion])
	// Numeric components too, because the DSL compares strings lexicographically and would
	// otherwise sort "1.9.0" after "1.28.0".
	require.Equal(t, 1, c[CKSDKMajor])
	require.Equal(t, 28, c[CKSDKMinor])
	require.Equal(t, 3, c[CKSDKPatch])

	// Always non-nil, so it is safe to chain onto.
	require.NotNil(t, ConstraintsFromContext(t.Context()))
}

func TestGetC_AmbientAppliesWhenCallerSuppliesNothing(t *testing.T) {
	// The point-3 requirement: ambient is held by the client and merged into every
	// evaluation, so a caller that passes nothing still gets it.
	c := newTestExprClient(t, nil, `
history.persistenceMaxQPS:
  defaultValue: 9000
  overrides:
    - matchString: '"zone" = "us-west-2"'
      matchResult: 18000
`)
	col := NewCollection(c, log.NewTestLogger())

	require.Equal(t, 18000, HistoryPersistenceMaxQPS.GetC(col)(nil))
	require.Equal(t, 18000, HistoryPersistenceMaxQPS.GetC(col)(NewConstraintsMap()))
	// and the ordinary accessor agrees
	require.Equal(t, 18000, HistoryPersistenceMaxQPS.Get(col)())
}

func TestGetC_CallerDimensions(t *testing.T) {
	c := newTestExprClient(t, nil, `
constraintKeys: [tenant]
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"namespace" = "canary" and "sdkMajor" > 1'
      matchResult: 2
    - matchString: '"tenant" = "acme"'
      matchResult: 3
    - matchString: '"namespace" = "canary"'
      matchResult: 4
`)
	col := NewCollection(c, log.NewTestLogger())
	get := MatchingHistoryMaxPageSize.GetC(col)

	// A per-request dimension the Client seam could never see.
	require.Equal(t, 2, get(ConstraintsWithNS("canary").With(CKSDKMajor, 2)))
	// First match in file order wins, so an old SDK in canary falls to the later override.
	require.Equal(t, 4, get(ConstraintsWithNS("canary").With(CKSDKMajor, 1)))
	// A dimension nothing in Temporal knows about, declared in the file and supplied by the
	// caller.
	require.Equal(t, 3, get(ConstraintsWithNS("other").With("tenant", "acme")))
	require.Equal(t, 1, get(ConstraintsWithNS("other")))

	// The same key read through the ordinary accessor sees no caller dimensions at all.
	require.Equal(t, 1, MatchingHistoryMaxPageSize.Get(col)("canary"))
}

func TestGetC_CallerShadowsAmbient(t *testing.T) {
	c := newTestExprClient(t, nil, `
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"zone" = "eu-west-1"'
      matchResult: 2
`)
	col := NewCollection(c, log.NewTestLogger())
	get := MatchingHistoryMaxPageSize.GetC(col)

	// ambient zone is us-west-2
	require.Equal(t, 1, get(nil))
	// the caller's value wins over ambient
	require.Equal(t, 2, get(NewConstraintsMap().With(CKZone, "eu-west-1")))
}

func TestGetC_FallsBackToFileConstrainedValues(t *testing.T) {
	// A call site that moves to GetC must keep honouring constrained values in the dynamic
	// config file, which means the known keys have to be projected back onto the precedence
	// list. Nothing here is expression-configured.
	inner := StaticClient{
		MatchingHistoryMaxPageSize.Key(): []ConstrainedValue{
			{Constraints: Constraints{Namespace: "canary"}, Value: 7},
			{Value: 8},
		},
		MatchingGetTasksBatchSize.Key(): []ConstrainedValue{
			{Constraints: Constraints{Namespace: "ns", TaskQueueName: "tq", TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY}, Value: 5},
			{Value: 6},
		},
	}
	restoreExprSettings(t)
	col := NewCollection(inner, log.NewTestLogger())

	require.Equal(t, 7, MatchingHistoryMaxPageSize.GetC(col)(ConstraintsWithNS("canary")))
	require.Equal(t, 8, MatchingHistoryMaxPageSize.GetC(col)(ConstraintsWithNS("other")))

	// task queue precedence, including the enum, projected out of the map
	tq := ConstraintsWithNS("ns").
		With(CKTaskQueueName, "tq").
		With(CKTaskQueueType, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	require.Equal(t, 5, MatchingGetTasksBatchSize.GetC(col)(tq))
	require.Equal(t, 6, MatchingGetTasksBatchSize.GetC(col)(ConstraintsWithNS("ns")))

	// the enum also accepted in its string form, as it is written in YAML
	tqStr := ConstraintsWithNS("ns").With(CKTaskQueueName, "tq").With(CKTaskQueueType, "Activity")
	require.Equal(t, 5, MatchingGetTasksBatchSize.GetC(col)(tqStr))
}

func TestGetC_ConstrainedDefaults(t *testing.T) {
	restoreExprSettings(t)
	col := NewCollection(NewNoopClient(), log.NewTestLogger())

	tq := ConstraintsWithNS("ns").
		With(CKTaskQueueName, primitives.PerNSWorkerTaskQueue).
		With(CKTaskQueueType, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	require.Equal(t, 1, MatchingNumTaskqueueReadPartitions.GetC(col)(tq))

	other := ConstraintsWithNS("ns").With(CKTaskQueueName, "ordinary")
	require.Equal(t, GlobalDefaultNumTaskQueuePartitions,
		MatchingNumTaskqueueReadPartitions.GetC(col)(other))
}

// Get and GetC must resolve a conflict the same way, whichever source wins. They reach the
// value by different routes — Get through Client.GetValue, GetC through the Evaluator — and
// an earlier revision had GetC short-circuit the constrained-default resolution, so the same
// config gave 16 through one accessor and 1 through the other.
func TestGetC_AgreesWithGetOnConflicts(t *testing.T) {
	inner := StaticClient{
		// deliberately more specific than the unconstrained expression value
		exprNamespaceInt.Key(): []ConstrainedValue{
			{Constraints: Constraints{Namespace: "canary"}, Value: 7},
			{Value: 8},
		},
		exprTQPartitions.Key(): []ConstrainedValue{{Value: 99}},
	}
	col := NewCollection(newTestExprClient(t, inner, `
matching.historyMaxPageSize:
  defaultValue: 42
matching.numTaskqueueReadPartitions:
  defaultValue: 16
`), log.NewTestLogger())

	t.Run("expression beats the dynamic config file, however constrained", func(t *testing.T) {
		require.Equal(t, 42, exprNamespaceInt.Get(col)("canary"))
		require.Equal(t, 42, exprNamespaceInt.GetC(col)(ConstraintsWithNS("canary")))
	})

	t.Run("a more specific constrained default still beats the expression", func(t *testing.T) {
		// One partition for the per-namespace worker task queue is a correctness invariant,
		// not a preference: a fleet-wide partition count must not override it.
		perNS := ConstraintsWithNS("ns").
			With(CKTaskQueueName, primitives.PerNSWorkerTaskQueue).
			With(CKTaskQueueType, enumspb.TASK_QUEUE_TYPE_WORKFLOW)

		require.Equal(t, 1, exprTQPartitions.Get(col)(
			"ns", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW))
		require.Equal(t, 1, exprTQPartitions.GetC(col)(perNS))
	})

	t.Run("and the expression applies where no constrained default does", func(t *testing.T) {
		ordinary := ConstraintsWithNS("ns").
			With(CKTaskQueueName, "ordinary").
			With(CKTaskQueueType, enumspb.TASK_QUEUE_TYPE_WORKFLOW)

		require.Equal(t, 16, exprTQPartitions.Get(col)(
			"ns", "ordinary", enumspb.TASK_QUEUE_TYPE_WORKFLOW))
		require.Equal(t, 16, exprTQPartitions.GetC(col)(ordinary))
	})
}

func TestGetC_UnknownConstraintKeyRejected(t *testing.T) {
	restoreExprSettings(t)
	c := NewConfiguratorClient(testExprAmbient, nil, log.NewTestLogger())

	err := c.LoadFile([]byte(`
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"sdkMnior" = 2'
      matchResult: 2
`))
	require.ErrorContains(t, err, "sdkMnior")
	require.ErrorContains(t, err, "expressionConstraints")

	// declaring it makes the same file load
	require.NoError(t, c.LoadFile([]byte(`
constraintKeys: [sdkMnior]
matching.historyMaxPageSize:
  defaultValue: 1
  overrides:
    - matchString: '"sdkMnior" = 2'
      matchResult: 2
`)))
}

func TestGetC_AmbientOnlyKeysUseTheFastPath(t *testing.T) {
	c := newTestExprClient(t, nil, `
history.persistenceMaxQPS:
  defaultValue: 1
  overrides:
    - matchString: '"zone" = "us-west-2"'
      matchResult: 2
matching.historyMaxPageSize:
  defaultValue: 3
  overrides:
    - matchString: '"namespace" = "x"'
      matchResult: 4
`)

	col := NewCollection(c, log.NewTestLogger())
	require.Equal(t, 2, HistoryPersistenceMaxQPS.GetC(col)(nil))
	require.Equal(t, 4, MatchingHistoryMaxPageSize.GetC(col)(ConstraintsWithNS("x")))

	// The ambient-only entry skips evaluation even when the caller supplies dimensions, as
	// long as none of them is one the entry actually tests.
	snap := c.snapshot.Load()
	ambientEntry := snap.entries[HistoryPersistenceMaxQPS.Key()]
	require.True(t, ambientEntry.ambientOnly)
	require.True(t, ambientEntry.canUseResolved(ConstraintsWithNS("anything")))
	// ...but not when the caller shadows one of them.
	require.False(t, ambientEntry.canUseResolved(NewConstraintsMap().With(CKZone, "eu-west-1")))

	perCaller := snap.entries[MatchingHistoryMaxPageSize.Key()]
	require.False(t, perCaller.ambientOnly)
	require.False(t, perCaller.canUseResolved(ConstraintsWithNS("x")))
	require.True(t, perCaller.canUseResolved(nil))
}

func TestGetC_DurationAndTypedValues(t *testing.T) {
	c := newTestExprClient(t, nil, `
matching.longPollExpirationInterval:
  defaultValue: 1m
  overrides:
    - matchString: '"namespace" = "slow"'
      matchResult: 30s
`)
	col := NewCollection(c, log.NewTestLogger())
	get := MatchingLongPollExpirationInterval.GetC(col)

	require.Equal(t, time.Minute, get(ConstraintsWithNS("fast")))
	require.Equal(t, 30*time.Second, get(ConstraintsWithNS("slow")))
}

func TestGetC_NoEvaluatorBehavesLikeGet(t *testing.T) {
	// A plain client that is not an Evaluator: GetC must be indistinguishable from Get.
	restoreExprSettings(t)
	col := NewCollection(StaticClient{MatchingHistoryMaxPageSize.Key(): 11}, log.NewTestLogger())

	require.Equal(t, 11, MatchingHistoryMaxPageSize.Get(col)("ns"))
	require.Equal(t, 11, MatchingHistoryMaxPageSize.GetC(col)(ConstraintsWithNS("ns")))
	require.Equal(t, 11, MatchingHistoryMaxPageSize.GetC(col)(nil))
}

func BenchmarkGetC(b *testing.B) {
	inner := StaticClient{
		MatchingHistoryMaxPageSize.Key(): []ConstrainedValue{
			{Value: 100},
			{Constraints: Constraints{Namespace: "ns-7"}, Value: 25},
		},
	}
	ambient := AmbientConstraints{Environment: "staging", AvailabilityZone: "us-west-2"}

	newClient := func(b *testing.B, yaml string) *ConfiguratorClient {
		c := NewConfiguratorClient(ambient, inner, log.NewNoopLogger())
		if err := c.LoadFile([]byte(yaml)); err != nil {
			b.Fatal(err)
		}
		return c
	}

	// The regression risk for the whole change: every setting nobody expression-configures
	// takes this path.
	b.Run("unconfigured key, nil constraints", func(b *testing.B) {
		col := NewCollection(newClient(b, "history.rps:\n  defaultValue: 1\n"), log.NewNoopLogger())
		get := MatchingHistoryMaxPageSize.GetC(col)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = get(nil)
		}
	})

	b.Run("ambient-only key, nil constraints", func(b *testing.B) {
		col := NewCollection(newClient(b, `
matching.historyMaxPageSize:
  defaultValue: 100
  overrides:
    - matchString: '"zone" = "us-west-2"'
      matchResult: 25
`), log.NewNoopLogger())
		get := MatchingHistoryMaxPageSize.GetC(col)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = get(nil)
		}
	})

	for _, n := range []int{1, 5} {
		b.Run("per-caller key, "+string(rune('0'+n))+" caller dimensions", func(b *testing.B) {
			col := NewCollection(newClient(b, `
matching.historyMaxPageSize:
  defaultValue: 100
  overrides:
    - matchString: '"namespace" = "ns-7" and "zone" = "us-west-2"'
      matchResult: 25
`), log.NewNoopLogger())
			get := MatchingHistoryMaxPageSize.GetC(col)
			cm := ConstraintsWithNS("ns-7")
			for i := 1; i < n; i++ {
				cm.With("pad"+string(rune('a'+i)), i)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = get(cm)
			}
		})
	}

	b.Run("building ConstraintsMap per request", func(b *testing.B) {
		ns := namespace.Name("ns-7")
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// Assign to a package-level sink: otherwise escape analysis keeps the map on the
			// stack and the benchmark reports an allocation cost real callers do not get.
			constraintsSink = ConstraintsWithNS(ns).With(CKSDKName, "temporal-go").With(CKSDKMajor, 1)
		}
	})
}

var constraintsSink ConstraintsMap
