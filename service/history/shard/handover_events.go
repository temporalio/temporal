package shard

import (
	otellog "go.opentelemetry.io/otel/log"
	"go.temporal.io/server/common/wideevents"
)

// NamespaceLifecycle phases emitted by the handover tracker. Each shard keeps its own
// replication watermark for a namespace in handover, and a handover only completes once every
// shard's watermark has been acked by the target. These two phases record when a shard takes a
// watermark and when it drops it, so a handover that stalls can be traced to the shards still
// holding one.
//
// Both fire only on an actual mutation of the tracker's map — a handful of times per shard per
// handover — not on the per-notification path that calls UpdateHandoverState for every namespace.
const (
	phaseHandoverWatermarkSet     = "shard_handover_watermark_set"
	phaseHandoverWatermarkRemoved = "shard_handover_watermark_removed"
)

// Reasons for a watermark set, distinguishing the three ways a shard arrives at one.
const (
	// watermarkAdded: the shard saw this namespace enter handover and took a watermark.
	watermarkAdded = "added"
	// watermarkUpdated: a newer namespace notification advanced the watermark.
	watermarkUpdated = "updated"
	// watermarkResolved: the shard was unacquired when it took the watermark, so it stored the
	// PendingMaxReplicationTaskID sentinel; acquiring the shard replaced it with a real task ID.
	watermarkResolved = "resolved_pending"
)

func emitHandoverWatermarkSet(
	logger otellog.Logger,
	shardID int32,
	nsName string,
	nsID string,
	info *namespaceHandOverInfo,
	reason string,
) {
	wideevents.Emit(logger, wideevents.NamespaceLifecyclePayload{
		Phase:       phaseHandoverWatermarkSet,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                shardID,
			"max_replication_task_id": info.MaxReplicationTaskID,
			"notification_version":    info.NotificationVersion,
			// pending means the shard was not acquired, so the watermark is the sentinel and
			// replication has not been notified yet.
			"pending": info.MaxReplicationTaskID == PendingMaxReplicationTaskID,
			"reason":  reason,
		},
	})
}

func emitHandoverWatermarkRemoved(
	logger otellog.Logger,
	shardID int32,
	nsName string,
	nsID string,
	removed *namespaceHandOverInfo,
	deletedFromDB bool,
) {
	wideevents.Emit(logger, wideevents.NamespaceLifecyclePayload{
		Phase:       phaseHandoverWatermarkRemoved,
		Namespace:   nsName,
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                shardID,
			"max_replication_task_id": removed.MaxReplicationTaskID,
			"notification_version":    removed.NotificationVersion,
			// deleted_from_db separates a namespace deletion from the normal exit out of the
			// handover replication state.
			"deleted_from_db": deletedFromDB,
		},
	})
}
