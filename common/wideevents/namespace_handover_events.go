package wideevents

import (
	"context"
	"errors"
	"time"
)

// The NamespaceLifecycle phases emitted for namespace handover, and the payload builders for
// each. A handover cannot complete until every shard's replication watermark has been acked by
// the target cluster, so the phases below cover both ends of that: the per-shard watermark
// bookkeeping on the history side, and the wait that blocks on it on the worker side.
//
// Phase values are the published contract that queries key on, so they live together here rather
// than next to their emitters. NamespaceLifecyclePayload.Phase stays a plain string: the
// vocabulary is deliberately open so out-of-tree emitters can define their own phases without a
// change to this package.
const (
	PhaseHandoverWatermarkSet     = "shard_handover_watermark_set"
	PhaseHandoverWatermarkRemoved = "shard_handover_watermark_removed"
	PhaseHandoverIncomplete       = "shard_handover_incomplete"
)

// Reasons for a watermark set, distinguishing the two ways a shard arrives at one.
const (
	// WatermarkAdded: the shard saw this namespace enter handover and took a watermark.
	WatermarkAdded = "added"
	// WatermarkUpdated: a newer namespace notification advanced the watermark.
	WatermarkUpdated = "updated"
)

// MaxLaggingShardsInSummary caps the per-shard list in HandoverIncomplete so a cluster with
// thousands of shards cannot produce an unbounded details blob. The true count is always in
// not_ready_count.
const MaxLaggingShardsInSummary = 512

// HandoverWatermarkSet reports a shard taking or advancing its replication watermark for a
// namespace entering handover. pending means the shard was not acquired, so the watermark is a
// sentinel and replication has not been notified yet.
func HandoverWatermarkSet(
	shardID int32,
	nsName string,
	nsID string,
	maxReplicationTaskID int64,
	notificationVersion int64,
	pending bool,
	reason string,
) NamespaceLifecyclePayload {
	return NamespaceLifecyclePayload{
		Phase:       PhaseHandoverWatermarkSet,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                shardID,
			"max_replication_task_id": maxReplicationTaskID,
			"notification_version":    notificationVersion,
			"pending":                 pending,
			"reason":                  reason,
		},
	}
}

// HandoverWatermarkRemoved reports a shard dropping its watermark. deletedFromDB separates a
// namespace deletion from the normal exit out of the handover replication state.
func HandoverWatermarkRemoved(
	shardID int32,
	nsName string,
	nsID string,
	maxReplicationTaskID int64,
	notificationVersion int64,
	deletedFromDB bool,
) NamespaceLifecyclePayload {
	return NamespaceLifecyclePayload{
		Phase:       PhaseHandoverWatermarkRemoved,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                shardID,
			"max_replication_task_id": maxReplicationTaskID,
			"notification_version":    notificationVersion,
			"deleted_from_db":         deletedFromDB,
		},
	}
}

// LaggingShard is one shard that had not caught up when a handover wait ended.
type LaggingShard struct {
	ShardID      int32 `json:"shard_id"`
	LaggingTasks int64 `json:"lagging_tasks"`
}

// HandoverLagSnapshot is a point-in-time view of how far each shard is from ready. The waiter
// overwrites it on every poll and emits whatever is left in it when the wait unwinds, so it
// describes the final state of the wait rather than an accumulation across polls.
type HandoverLagSnapshot struct {
	TotalShards int
	ReadyCount  int
	// NotReadyCount is the true count; LaggingShards may be truncated.
	NotReadyCount int
	// MissingHandoverInfoCount is the subset of not-ready shards whose namespace cache has not
	// picked up the handover yet, so they carry no watermark rather than a lagging one.
	MissingHandoverInfoCount int
	MaxLaggingTasks          int64
	MaxLaggingTasksShardID   int32
	LaggingShards            []LaggingShard
}

// AddLaggingShard records a not-ready shard, up to MaxLaggingShardsInSummary of them.
func (s *HandoverLagSnapshot) AddLaggingShard(shardID int32, laggingTasks int64) {
	if len(s.LaggingShards) >= MaxLaggingShardsInSummary {
		return
	}
	s.LaggingShards = append(s.LaggingShards, LaggingShard{ShardID: shardID, LaggingTasks: laggingTasks})
}

// HandoverIncomplete reports a handover wait that ended with shards still behind. A wait that
// succeeds leaves no laggards, so callers skip the emit entirely and the happy path is silent.
//
// A not-ready shard with lagging_tasks == 0 is the missing-handover-info case; any other
// not-ready shard is behind by lagging_tasks.
func HandoverIncomplete(
	nsName string,
	nsID string,
	remoteCluster string,
	snapshot *HandoverLagSnapshot,
	elapsed time.Duration,
	exitErr error,
) NamespaceLifecyclePayload {
	details := map[string]any{
		"remote_cluster":              remoteCluster,
		"total_shards":                snapshot.TotalShards,
		"ready_count":                 snapshot.ReadyCount,
		"not_ready_count":             snapshot.NotReadyCount,
		"missing_handover_info_count": snapshot.MissingHandoverInfoCount,
		"max_lagging_tasks":           snapshot.MaxLaggingTasks,
		"max_lagging_tasks_shard_id":  snapshot.MaxLaggingTasksShardID,
		"lagging_shards":              snapshot.LaggingShards,
		"lagging_shards_truncated":    snapshot.NotReadyCount > len(snapshot.LaggingShards),
		"elapsed_seconds":             elapsed.Seconds(),
		"exit_reason":                 handoverExitReason(exitErr),
	}
	if exitErr != nil {
		details["exit_error"] = exitErr.Error()
	}
	return NamespaceLifecyclePayload{
		Phase:       PhaseHandoverIncomplete,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details:     details,
	}
}

// handoverExitReason separates the wait being killed while shards were still behind from a failed
// status check, since the two point at different problems.
func handoverExitReason(err error) string {
	switch {
	case err == nil:
		return "returned_ready"
	case errors.Is(err, context.Canceled):
		return "context_canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "error"
	}
}
