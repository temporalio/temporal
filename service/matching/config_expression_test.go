package matching

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/tqid"
)

// These tests cover the layer 2 part of the constraint-expression prototype: per-task-queue
// settings resolved against a Collection view that carries the partition's own dimensions.
// None of these dimensions has a field in dynamicconfig.Constraints, so none of them can
// target a setting through the file based client.

func newExprMatchingConfig(t *testing.T, exprYaml string) *Config {
	t.Helper()
	evaluator := dynamicconfig.NewConfiguratorEvaluator(
		dynamicconfig.AmbientConstraints{Environment: "staging", ServiceName: "matching"},
		log.NewTestLogger(),
	)
	require.NoError(t, evaluator.LoadFile([]byte(exprYaml)))
	return NewConfig(dynamicconfig.NewCollectionWithEvaluator(
		dynamicconfig.NewNoopClient(), log.NewTestLogger(), evaluator))
}

func exprTaskQueue() *tqid.TaskQueue {
	return tqid.UnsafeTaskQueueFamily("nsid", "tq").TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW)
}

func TestPartitionConstraints(t *testing.T) {
	tq := exprTaskQueue()

	root := partitionConstraints(tq.RootPartition())
	require.Equal(t, 0, root["taskQueuePartitionID"])
	require.Equal(t, true, root["taskQueueIsRoot"])
	require.Equal(t, false, root["taskQueueIsSticky"])
	require.Equal(t, "Normal", root["taskQueueKind"])

	child := partitionConstraints(tq.NormalPartition(3))
	require.Equal(t, 3, child["taskQueuePartitionID"])
	require.Equal(t, false, child["taskQueueIsRoot"])

	sticky := partitionConstraints(tq.StickyPartition("sticky-name"))
	require.Equal(t, true, sticky["taskQueueIsSticky"])
	require.Equal(t, "Sticky", sticky["taskQueueKind"])
	// sticky partitions are not numbered
	require.NotContains(t, sticky, "taskQueuePartitionID")
}

func TestTaskQueueConfig_PartitionIDConstraint(t *testing.T) {
	cfg := newExprMatchingConfig(t, `
matching.getTasksBatchSize:
  defaultValue: 100
  overrides:
    - matchString: '"taskQueuePartitionID" > 2'
      matchResult: 25
`)
	tq := exprTaskQueue()

	require.Equal(t, 100, newTaskQueueConfig(tq.NormalPartition(0), cfg, "ns").GetTasksBatchSize())
	require.Equal(t, 100, newTaskQueueConfig(tq.NormalPartition(2), cfg, "ns").GetTasksBatchSize())
	require.Equal(t, 25, newTaskQueueConfig(tq.NormalPartition(3), cfg, "ns").GetTasksBatchSize())
}

func TestTaskQueueConfig_RootPartitionConstraint(t *testing.T) {
	// Forwarding only happens on non-root partitions, so this is a distinction the
	// task queue precedence order cannot draw.
	cfg := newExprMatchingConfig(t, `
matching.maxWaitForPollerBeforeFwd:
  defaultValue: 1s
  overrides:
    - matchString: '"taskQueueIsRoot" = "true"'
      matchResult: 5s
`)
	tq := exprTaskQueue()

	require.Equal(t, 5*time.Second,
		newTaskQueueConfig(tq.RootPartition(), cfg, "ns").MaxWaitForPollerBeforeFwd())
	require.Equal(t, time.Second,
		newTaskQueueConfig(tq.NormalPartition(1), cfg, "ns").MaxWaitForPollerBeforeFwd())
}

func TestTaskQueueConfig_StickyAndNamespaceCombined(t *testing.T) {
	// Ad-hoc dimensions compose with the ordinary precedence-derived ones.
	cfg := newExprMatchingConfig(t, `
matching.syncMatchWaitDuration:
  defaultValue: 200ms
  overrides:
    - matchString: '"taskQueueIsSticky" = "true" and "namespace" = "hot-ns"'
      matchResult: 1s
`)
	tq := exprTaskQueue()

	require.Equal(t, time.Second,
		newTaskQueueConfig(tq.StickyPartition("s"), cfg, "hot-ns").SyncMatchWaitDuration())
	require.Equal(t, 200*time.Millisecond,
		newTaskQueueConfig(tq.StickyPartition("s"), cfg, "other-ns").SyncMatchWaitDuration())
	require.Equal(t, 200*time.Millisecond,
		newTaskQueueConfig(tq.RootPartition(), cfg, "hot-ns").SyncMatchWaitDuration())
}

func TestTaskQueueConfig_NoEvaluatorIsUnchanged(t *testing.T) {
	// The default wiring: no evaluator at all. Values must come from the compiled-in
	// defaults exactly as before.
	cfg := NewConfig(dynamicconfig.NewNoopCollection())
	tq := exprTaskQueue()

	tqc := newTaskQueueConfig(tq.NormalPartition(3), cfg, "ns")
	require.Equal(t,
		dynamicconfig.MatchingGetTasksBatchSize.Get(dynamicconfig.NewNoopCollection())(
			"ns", tq.Name(), tq.TaskType()),
		tqc.GetTasksBatchSize())
	require.Equal(t,
		dynamicconfig.MatchingMaxWaitForPollerBeforeFwd.Get(dynamicconfig.NewNoopCollection())(
			"ns", tq.Name(), tq.TaskType()),
		tqc.MaxWaitForPollerBeforeFwd())
}
