package dynamicconfig_test

import (
	"fmt"
	"strings"
	"testing"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
)

// BenchmarkEvaluator measures the read cost of the constraint-expression prototype against
// the existing ConstrainedValue lookup. The comparison that matters most is
// "absent from expression config" against "no evaluator": that difference is paid by every
// one of the ~677 settings on every read, whether or not anyone uses expressions.
func BenchmarkEvaluator(b *testing.B) {
	const key = "matching.getTasksBatchSize"
	setting := dynamicconfig.MatchingGetTasksBatchSize

	// The equivalent of the expression config below, expressed the way it must be today:
	// one exact-match ConstrainedValue per combination.
	client := dynamicconfig.StaticClient{
		setting.Key(): []dynamicconfig.ConstrainedValue{
			{Value: 100},
			{Constraints: dynamicconfig.Constraints{Namespace: "ns-7"}, Value: 25},
		},
		dynamicconfig.HistoryRPS.Key(): []dynamicconfig.ConstrainedValue{{Value: 100}},
	}

	// exprWithOverrides builds an expression config with n non-matching overrides in front
	// of one that matches, so the cost of walking the override list is visible.
	exprWithOverrides := func(n int) string {
		var sb strings.Builder
		fmt.Fprintf(&sb, "%s:\n  defaultValue: 100\n  overrides:\n", key)
		for i := 0; i < n; i++ {
			fmt.Fprintf(&sb, "    - matchString: '\"namespace\" = \"ns-%d\"'\n      matchResult: %d\n", i, i)
		}
		sb.WriteString("    - matchString: '\"env\" = \"staging\" and (\"zone\" = \"us-west-1\" or \"zone\" = \"us-west-2\")'\n      matchResult: 25\n")
		return sb.String()
	}

	newEvaluator := func(b *testing.B, yaml string) dynamicconfig.Evaluator {
		e := dynamicconfig.NewConfiguratorEvaluator(dynamicconfig.AmbientConstraints{
			Environment:      "staging",
			AvailabilityZone: "us-west-2",
			ClusterName:      "active",
			ServiceName:      "matching",
		}, log.NewNoopLogger())
		if err := e.LoadFile([]byte(yaml)); err != nil {
			b.Fatal(err)
		}
		return e
	}

	run := func(b *testing.B, col *dynamicconfig.Collection) {
		b.ReportAllocs()
		get := setting.Get(col)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = get("ns-7", "tq", enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		}
	}

	// Baseline: today's behaviour.
	b.Run("no evaluator", func(b *testing.B) {
		run(b, dynamicconfig.NewCollection(client, log.NewNoopLogger()))
	})

	// The cost paid by every setting that is not expression configured.
	b.Run("evaluator present, key absent", func(b *testing.B) {
		e := newEvaluator(b, "history.rps:\n  defaultValue: 1\n")
		run(b, dynamicconfig.NewCollectionWithEvaluator(client, log.NewNoopLogger(), e))
	})

	for _, n := range []int{0, 1, 5, 20} {
		b.Run(fmt.Sprintf("expression, %d overrides scanned", n), func(b *testing.B) {
			e := newEvaluator(b, exprWithOverrides(n))
			run(b, dynamicconfig.NewCollectionWithEvaluator(client, log.NewNoopLogger(), e))
		})
	}

	// A derived view adds ad-hoc dimensions to every constraint map it builds.
	b.Run("expression, 5 overrides, WithConstraints view", func(b *testing.B) {
		e := newEvaluator(b, exprWithOverrides(5))
		col := dynamicconfig.NewCollectionWithEvaluator(client, log.NewNoopLogger(), e)
		run(b, col.WithConstraints(map[string]any{"taskQueuePartitionID": 3, "taskQueueIsRoot": false}))
	})
}
