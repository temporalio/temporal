package wideevents

import (
	"context"
	"errors"
	"time"

	otellog "go.opentelemetry.io/otel/log"
)

// The NamespaceLifecycle phases and their emitters. Phase values are the published contract that
// queries key on, and the details each phase carries is the rest of that contract, so both live
// here rather than at the call sites.

// Namespace handover. A handover cannot complete until every shard's replication watermark has
// been acked by the target, so these cover both ends of it: the per-shard watermark bookkeeping
// on the history side, and the wait that blocks on it on the worker side.
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

// EmitHandoverWatermarkSet reports a shard taking or advancing its replication watermark for a
// namespace entering handover. pending means the shard was not acquired, so the watermark is a
// sentinel and replication has not been notified yet.
func EmitHandoverWatermarkSet(
	logger otellog.Logger,
	shardID int32,
	nsName string,
	nsID string,
	maxReplicationTaskID int64,
	notificationVersion int64,
	pending bool,
	reason string,
) {
	Emit(logger, NamespaceLifecyclePayload{
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
	})
}

// EmitHandoverWatermarkRemoved reports a shard dropping its watermark. deletedFromDB separates a
// namespace deletion from the normal exit out of the handover replication state.
func EmitHandoverWatermarkRemoved(
	logger otellog.Logger,
	shardID int32,
	nsName string,
	nsID string,
	maxReplicationTaskID int64,
	notificationVersion int64,
	deletedFromDB bool,
) {
	Emit(logger, NamespaceLifecyclePayload{
		Phase:       PhaseHandoverWatermarkRemoved,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                shardID,
			"max_replication_task_id": maxReplicationTaskID,
			"notification_version":    notificationVersion,
			"deleted_from_db":         deletedFromDB,
		},
	})
}

// MaxLaggingShardsInSummary caps the per-shard list so a cluster with thousands of shards cannot
// produce an unbounded details blob. NotReadyCount is always the true count.
const MaxLaggingShardsInSummary = 512

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
	// NotReadyCount is the true count; LaggingShards is capped.
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

// EmitHandoverIncomplete reports a handover wait that ended with shards still behind. A wait that
// succeeds leaves no laggards, so this is a no-op and the happy path stays silent.
//
// A not-ready shard with lagging_tasks == 0 is the missing-handover-info case; any other
// not-ready shard is behind by lagging_tasks.
func EmitHandoverIncomplete(
	logger otellog.Logger,
	nsName string,
	nsID string,
	remoteCluster string,
	snapshot *HandoverLagSnapshot,
	elapsed time.Duration,
	exitErr error,
) {
	if snapshot.NotReadyCount == 0 {
		return
	}

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

	Emit(logger, NamespaceLifecyclePayload{
		Phase:       PhaseHandoverIncomplete,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details:     details,
	})
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
