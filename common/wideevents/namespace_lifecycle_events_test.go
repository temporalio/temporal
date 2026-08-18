package wideevents

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
)

func lagDetails(t *testing.T, rec log.Record) map[string]any {
	t.Helper()
	var raw string
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		if kv.Key == "details" {
			raw = kv.Value.AsString()
		}
		return true
	})
	var out map[string]any
	require.NoError(t, json.Unmarshal([]byte(raw), &out))
	return out
}

func attrValue(rec log.Record, key string) string {
	var out string
	rec.WalkAttributes(func(kv log.KeyValue) bool {
		if kv.Key == key {
			out = kv.Value.AsString()
		}
		return true
	})
	return out
}

// A completed handover leaves no laggards, so nothing is emitted.
func TestEmitHandoverIncompleteSilentWhenReady(t *testing.T) {
	lg := &captureLogger{}

	EmitHandoverIncomplete(lg, "ns", "ns-id", "target", &HandoverLagSnapshot{TotalShards: 4}, time.Second, nil)

	require.Empty(t, lg.records)
}

func TestEmitHandoverIncompleteNamesLaggingShards(t *testing.T) {
	lg := &captureLogger{}

	snapshot := &HandoverLagSnapshot{
		TotalShards:              4,
		ReadyCount:               2,
		NotReadyCount:            2,
		MissingHandoverInfoCount: 1,
		MaxLaggingTasks:          42,
		MaxLaggingTasksShardID:   3,
	}
	snapshot.AddLaggingShard(1, 0)
	snapshot.AddLaggingShard(3, 42)

	EmitHandoverIncomplete(lg, "ns", "ns-id", "target", snapshot, 30*time.Second, context.Canceled)

	require.Len(t, lg.records, 1)
	require.Equal(t, PhaseHandoverIncomplete, attrValue(lg.records[0], "phase"))
	require.Equal(t, "ns-id", attrValue(lg.records[0], "namespace_id"))

	d := lagDetails(t, lg.records[0])
	require.EqualValues(t, 4, d["total_shards"])
	require.EqualValues(t, 2, d["ready_count"])
	require.EqualValues(t, 2, d["not_ready_count"])
	require.EqualValues(t, 1, d["missing_handover_info_count"])
	require.EqualValues(t, 42, d["max_lagging_tasks"])
	require.EqualValues(t, 3, d["max_lagging_tasks_shard_id"])
	require.EqualValues(t, 30, d["elapsed_seconds"])
	require.Equal(t, "context_canceled", d["exit_reason"])
	require.Equal(t, "target", d["remote_cluster"])
	require.Equal(t, false, d["lagging_shards_truncated"])

	shards := d["lagging_shards"].([]any)
	require.Len(t, shards, 2)
	require.EqualValues(t, 1, shards[0].(map[string]any)["shard_id"])
	require.EqualValues(t, 42, shards[1].(map[string]any)["lagging_tasks"])
}

// A killed wait and a failed status check stay distinguishable.
func TestEmitHandoverIncompleteExitReason(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"canceled", context.Canceled, "context_canceled"},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded"},
		{"rpc", errors.New("shard 3 missing remote cluster"), "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lg := &captureLogger{}

			EmitHandoverIncomplete(lg, "ns", "ns-id", "target",
				&HandoverLagSnapshot{TotalShards: 1, NotReadyCount: 1}, time.Second, tc.err)

			require.Len(t, lg.records, 1)
			d := lagDetails(t, lg.records[0])
			require.Equal(t, tc.want, d["exit_reason"])
			require.Equal(t, tc.err.Error(), d["exit_error"])
		})
	}
}

// The list is capped; not_ready_count stays true.
func TestEmitHandoverIncompleteTruncates(t *testing.T) {
	lg := &captureLogger{}

	snapshot := &HandoverLagSnapshot{TotalShards: 4096, NotReadyCount: 4096}
	for i := range MaxLaggingShardsInSummary + 10 {
		snapshot.AddLaggingShard(int32(i), 1)
	}
	require.Len(t, snapshot.LaggingShards, MaxLaggingShardsInSummary)

	EmitHandoverIncomplete(lg, "ns", "ns-id", "target", snapshot, time.Second, context.Canceled)

	d := lagDetails(t, lg.records[0])
	require.EqualValues(t, 4096, d["not_ready_count"])
	require.Len(t, d["lagging_shards"].([]any), MaxLaggingShardsInSummary)
	require.Equal(t, true, d["lagging_shards_truncated"])
}

func TestEmitHandoverWatermarkEvents(t *testing.T) {
	lg := &captureLogger{}

	EmitHandoverWatermarkSet(lg, 7, "ns", "ns-id", 100, 2, false, WatermarkAdded)
	EmitHandoverWatermarkRemoved(lg, 7, "ns", "ns-id", 100, 2, true)

	require.Len(t, lg.records, 2)

	require.Equal(t, PhaseHandoverWatermarkSet, attrValue(lg.records[0], "phase"))
	set := lagDetails(t, lg.records[0])
	require.EqualValues(t, 7, set["shard_id"])
	require.EqualValues(t, 100, set["max_replication_task_id"])
	require.Equal(t, false, set["pending"])
	require.Equal(t, WatermarkAdded, set["reason"])

	require.Equal(t, PhaseHandoverWatermarkRemoved, attrValue(lg.records[1], "phase"))
	require.Equal(t, true, lagDetails(t, lg.records[1])["deleted_from_db"])
}

func TestEmitNamespaceRegistered(t *testing.T) {
	lg := &captureLogger{}

	EmitNamespaceRegistered(lg, NamespaceRegisteredInput{
		Namespace:   "ns",
		NamespaceID: "ns-id",
		Fields:      NamespaceStateFields{ActiveCluster: "clusterA", IsGlobalNamespace: true, FailoverVersion: 7},
		Requested:   NamespaceStateFields{Description: "requested-desc"},
	})

	require.Len(t, lg.records, 1)
	require.Equal(t, PhaseNamespaceRegistered, attrValue(lg.records[0], "phase"))
	require.Equal(t, "ns-id", attrValue(lg.records[0], "namespace_id"))

	after := lagDetails(t, lg.records[0])["after"].(map[string]any)
	require.Equal(t, "clusterA", after["active_cluster"])
	require.Equal(t, true, after["is_global_namespace"])
	require.EqualValues(t, 7, after["failover_version"])

	requested := lagDetails(t, lg.records[0])["requested"].(map[string]any)
	require.Equal(t, "requested-desc", requested["description"])
}

// A failover is a namespace_updated with is_failover set; the before/after snapshots carry the
// active-cluster and failover-version deltas.
func TestEmitNamespaceUpdatedCarriesBeforeAfterAndFlags(t *testing.T) {
	lg := &captureLogger{}

	EmitNamespaceUpdated(lg, NamespaceUpdatedInput{
		Namespace:                 "ns",
		NamespaceID:               "ns-id",
		IsFailover:                true,
		PromoteNamespaceRequested: true,
		DeleteBadBinary:           "cksum-old",
		RequestedFields:           []string{"active_cluster", "delete_bad_binary", "promote_namespace"},
		Before:                    NamespaceStateFields{ActiveCluster: "clusterA", FailoverVersion: 10},
		After:                     NamespaceStateFields{ActiveCluster: "clusterB", FailoverVersion: 21},
		Requested:                 NamespaceStateFields{ActiveCluster: "clusterB"},
	})

	require.Len(t, lg.records, 1)
	require.Equal(t, PhaseNamespaceUpdated, attrValue(lg.records[0], "phase"))

	d := lagDetails(t, lg.records[0])
	require.Equal(t, true, d["is_failover"])
	require.Equal(t, false, d["is_promotion"])
	require.Equal(t, true, d["promote_namespace_requested"])
	require.Equal(t, "cksum-old", d["delete_bad_binary"])
	require.Equal(t, []any{"active_cluster", "delete_bad_binary", "promote_namespace"}, d["requested_fields"])
	require.Equal(t, "clusterA", d["before"].(map[string]any)["active_cluster"])
	require.Equal(t, "clusterB", d["after"].(map[string]any)["active_cluster"])
	require.EqualValues(t, 21, d["after"].(map[string]any)["failover_version"])
	require.Equal(t, "clusterB", d["requested"].(map[string]any)["active_cluster"])
}

// A workflow-rule create/delete reuses namespace_updated: the directive names the rule, the
// before/after workflow_rule_ids carry the resulting set. Only one directive is populated per op.
func TestEmitNamespaceUpdatedWorkflowRuleDirectives(t *testing.T) {
	created := &captureLogger{}
	EmitNamespaceUpdated(created, NamespaceUpdatedInput{
		Namespace:                 "ns",
		NamespaceID:               "ns-id",
		WorkflowRuleCreated:       "rule-x",
		WorkflowRuleCreatedDetail: `spec:{id:"rule-x"} created_by_identity:"alice"`,
		WorkflowRuleForceScan:     true,
		WorkflowRuleRequestID:     "request-id",
		Before:                    NamespaceStateFields{WorkflowRuleIDs: []string{"rule-a"}},
		After:                     NamespaceStateFields{WorkflowRuleIDs: []string{"rule-a", "rule-x"}},
	})
	dc := lagDetails(t, created.records[0])
	require.Equal(t, "rule-x", dc["workflow_rule_created"])
	require.Equal(t, `spec:{id:"rule-x"} created_by_identity:"alice"`, dc["workflow_rule_created_detail"])
	require.Equal(t, true, dc["workflow_rule_force_scan"])
	require.Equal(t, "request-id", dc["workflow_rule_request_id"])
	require.Empty(t, dc["workflow_rule_deleted"])
	require.Empty(t, dc["workflow_rule_deleted_detail"])
	require.Equal(t, []any{"rule-a", "rule-x"}, dc["after"].(map[string]any)["workflow_rule_ids"])

	deleted := &captureLogger{}
	EmitNamespaceUpdated(deleted, NamespaceUpdatedInput{
		Namespace:                 "ns",
		NamespaceID:               "ns-id",
		WorkflowRuleDeleted:       "rule-x",
		WorkflowRuleDeletedDetail: `spec:{id:"rule-x"} created_by_identity:"alice"`,
		Before:                    NamespaceStateFields{WorkflowRuleIDs: []string{"rule-a", "rule-x"}},
		After:                     NamespaceStateFields{WorkflowRuleIDs: []string{"rule-a"}},
	})
	dd := lagDetails(t, deleted.records[0])
	require.Equal(t, "rule-x", dd["workflow_rule_deleted"])
	require.Equal(t, `spec:{id:"rule-x"} created_by_identity:"alice"`, dd["workflow_rule_deleted_detail"])
	require.Empty(t, dd["workflow_rule_created"])
	require.Empty(t, dd["workflow_rule_created_detail"])
}

func TestEmitNamespaceRenamed(t *testing.T) {
	lg := &captureLogger{}

	EmitNamespaceRenamed(lg, NamespaceRenamedInput{Namespace: "ns", NamespaceID: "ns-id", RenamedTo: "ns-deleted-abc"})

	require.Len(t, lg.records, 1)
	require.Equal(t, PhaseNamespaceRenamed, attrValue(lg.records[0], "phase"))
	require.Equal(t, "ns", attrValue(lg.records[0], "namespace"))
	require.Equal(t, "ns-id", attrValue(lg.records[0], "namespace_id"))
	require.Equal(t, "ns-deleted-abc", lagDetails(t, lg.records[0])["renamed_to"])
}
