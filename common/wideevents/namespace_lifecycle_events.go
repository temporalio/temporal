package wideevents

import (
	"context"
	"errors"
	"time"

	otellog "go.opentelemetry.io/otel/log"
)

// NamespaceLifecycle phases and their emitters. The phase values and the details each carries are
// the published contract, so both live here rather than at the call sites.

// Namespace handover: the per-shard watermark bookkeeping on the history side, and the wait that
// blocks on it on the worker side.
const (
	PhaseHandoverWatermarkSet     = "shard_handover_watermark_set"
	PhaseHandoverWatermarkRemoved = "shard_handover_watermark_removed"
	PhaseHandoverIncomplete       = "shard_handover_incomplete"
)

// Why a shard took or advanced its watermark.
const (
	// WatermarkAdded: the namespace entered handover.
	WatermarkAdded = "added"
	// WatermarkUpdated: a newer namespace notification advanced it.
	WatermarkUpdated = "updated"
)

// EmitHandoverWatermarkSet reports a shard taking or advancing its watermark. pending means the
// shard was unacquired, so the watermark is a sentinel and replication has not been notified.
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
// namespace deletion from a normal exit out of handover.
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

// MaxLaggingShardsInSummary caps the per-shard list; NotReadyCount is always the true count.
const MaxLaggingShardsInSummary = 512

// LaggingShard is one shard that had not caught up when a handover wait ended.
type LaggingShard struct {
	ShardID      int32 `json:"shard_id"`
	LaggingTasks int64 `json:"lagging_tasks"`
}

// HandoverLagSnapshot is the latest poll's view of which shards are behind. The waiter replaces it
// each poll, so it holds the final state of the wait rather than an accumulation.
type HandoverLagSnapshot struct {
	TotalShards int
	ReadyCount  int
	// NotReadyCount is the true count; LaggingShards is capped.
	NotReadyCount int
	// MissingHandoverInfoCount: not-ready shards holding no watermark yet, rather than a lagging one.
	MissingHandoverInfoCount int
	MaxLaggingTasks          int64
	MaxLaggingTasksShardID   int32
	LaggingShards            []LaggingShard
}

// NewHandoverLagSnapshot returns an empty snapshot with room for totalShards laggards.
func NewHandoverLagSnapshot(totalShards, missingHandoverInfoCount int) HandoverLagSnapshot {
	return HandoverLagSnapshot{
		TotalShards:              totalShards,
		MissingHandoverInfoCount: missingHandoverInfoCount,
		LaggingShards:            make([]LaggingShard, 0, min(totalShards, MaxLaggingShardsInSummary)),
	}
}

// AddLaggingShard records a not-ready shard, up to MaxLaggingShardsInSummary of them.
func (s *HandoverLagSnapshot) AddLaggingShard(shardID int32, laggingTasks int64) {
	if len(s.LaggingShards) >= MaxLaggingShardsInSummary {
		return
	}
	s.LaggingShards = append(s.LaggingShards, LaggingShard{ShardID: shardID, LaggingTasks: laggingTasks})
}

// EmitHandoverIncomplete reports a wait that ended with shards still behind. A successful wait
// leaves no laggards, so this is a no-op and the happy path stays silent.
//
// lagging_tasks == 0 on a not-ready shard is the missing-handover-info case.
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

// handoverExitReason separates the wait being killed from a failed status check.
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
