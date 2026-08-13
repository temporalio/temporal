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

// Namespace administration phases: register/update served by the frontend, plus the delete pipeline
// run by the worker. Each records a namespace metadata mutation, emitted once per successful
// mutation on the cluster that served the request — as opposed to the history and replication events,
// which track how that change propagates and takes effect.
//
// The serving cluster varies by op: for a global namespace RegisterNamespace and DeprecateNamespace
// are master-cluster only, but UpdateNamespace is not — a failover is issued on the cluster becoming
// active. Deletion is never replicated, so its pipeline runs per cluster.
//
// Everything that flows through UpdateNamespace — a config change, a state transition (DEPRECATED
// via DeprecateNamespace, DELETED via UpdateNamespace), a local->global promotion, or an
// active-cluster failover — is a namespace_updated event. Promotion and failover are marked with the
// is_promotion / is_failover flags rather than separate phases; the before/after field snapshots
// already carry the distinguishing data (active cluster, failover version, failover history).
//
// namespace_deleted is emitted from the worker delete pipeline (not the frontend): namespace
// deletion is local and never replicated, so it can only be observed there. See
// service/worker/deletenamespace.
//
// The register/update input structs are built by the frontend from its domain objects (see
// service/frontend/namespace_lifecycle_events.go), mirroring how service/history/replication relates
// to its handlers.
const (
	PhaseNamespaceRegistered = "namespace_registered"
	PhaseNamespaceUpdated    = "namespace_updated"
	PhaseNamespaceDeleted    = "namespace_deleted"
)

// FailoverHistoryEntry is one entry of a global namespace's failover history. It mirrors
// persistencespb.FailoverStatus in a form this package can carry without importing persistence.
type FailoverHistoryEntry struct {
	FailoverVersion int64  `json:"failover_version"`
	FailoverTime    string `json:"failover_time"`
}

// NamespaceStateFields is the snapshot of a namespace's fields the lifecycle events report.
// namespace_updated carries a Before and an After; namespace_registered carries only the created
// state.
type NamespaceStateFields struct {
	Description                 string                 `json:"description"`
	State                       string                 `json:"state"`
	IsGlobalNamespace           bool                   `json:"is_global_namespace"`
	ConfigVersion               int64                  `json:"config_version"`
	FailoverVersion             int64                  `json:"failover_version"`
	FailoverNotificationVersion int64                  `json:"failover_notification_version"`
	FailoverEndTime             string                 `json:"failover_end_time"`
	Retention                   string                 `json:"retention"`
	HistoryArchivalState        string                 `json:"history_archival_state"`
	VisibilityArchivalState     string                 `json:"visibility_archival_state"`
	ActiveCluster               string                 `json:"active_cluster"`
	Clusters                    []string               `json:"clusters"`
	ReplicationState            string                 `json:"replication_state"`
	FailoverHistory             []FailoverHistoryEntry `json:"failover_history"`
}

// NamespaceRegisteredInput is the input to EmitNamespaceRegistered.
type NamespaceRegisteredInput struct {
	Namespace   string
	NamespaceID string
	Fields      NamespaceStateFields
}

// NamespaceUpdatedInput is the input to EmitNamespaceUpdated. IsFailover marks an active-cluster
// change; IsPromotion marks a local->global promotion. Before/After carry the full field snapshots.
type NamespaceUpdatedInput struct {
	Namespace   string
	NamespaceID string
	IsFailover  bool
	IsPromotion bool
	Before      NamespaceStateFields
	After       NamespaceStateFields
}

// EmitNamespaceRegistered emits a namespace_registered event for a newly persisted namespace.
func EmitNamespaceRegistered(logger otellog.Logger, in NamespaceRegisteredInput) {
	Emit(logger, NamespaceLifecyclePayload{
		Phase:       PhaseNamespaceRegistered,
		Namespace:   in.Namespace,
		NamespaceID: in.NamespaceID,
		Details:     map[string]any{"after": in.Fields},
	})
}

// EmitNamespaceUpdated emits a namespace_updated event for a persisted namespace mutation.
func EmitNamespaceUpdated(logger otellog.Logger, in NamespaceUpdatedInput) {
	Emit(logger, NamespaceLifecyclePayload{
		Phase:       PhaseNamespaceUpdated,
		Namespace:   in.Namespace,
		NamespaceID: in.NamespaceID,
		Details: map[string]any{
			"is_failover":  in.IsFailover,
			"is_promotion": in.IsPromotion,
			"before":       in.Before,
			"after":        in.After,
		},
	})
}

// NamespaceDeletedInput is the input to EmitNamespaceDeleted. Namespace is the original name;
// RenamedTo is the tombstone name the record is renamed to as part of deletion.
type NamespaceDeletedInput struct {
	Namespace   string
	NamespaceID string
	RenamedTo   string
}

// EmitNamespaceDeleted emits a namespace_deleted event when a namespace is deleted. It is emitted at
// the point the namespace is renamed to its tombstone name (see service/worker/deletenamespace),
// which is when it ceases to exist under its real name.
func EmitNamespaceDeleted(logger otellog.Logger, in NamespaceDeletedInput) {
	Emit(logger, NamespaceLifecyclePayload{
		Phase:       PhaseNamespaceDeleted,
		Namespace:   in.Namespace,
		NamespaceID: in.NamespaceID,
		Details:     map[string]any{"renamed_to": in.RenamedTo},
	})
}
