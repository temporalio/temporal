package shard

import (
	"math"

	otellog "go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/wideevents"
)

const (
	// PendingMaxReplicationTaskID is a sentinel value indicating the shard state is not
	// yet acquired and the real max replication task ID is unknown.
	PendingMaxReplicationTaskID = math.MaxInt64
)

// HandoverTracker tracks namespace handover state on a shard. It manages the mapping
// of namespaces to replication watermarks during handover.
//
// Implementations must NOT hold their own locks — all methods are called within
// ContextImpl's existing write lock.
type HandoverTracker interface {
	// UpdateHandoverState processes a namespace state change.
	UpdateHandoverState(ns *namespace.Namespace, deletedFromDB bool)

	// IsInHandover returns true if operations for this namespace+workflowID should be
	// blocked due to handover.
	IsInHandover(namespaceName namespace.Name, workflowID string) bool

	// GetHandoverNamespaces returns handover info for the GetReplicationStatus RPC.
	// The key format is implementation-defined.
	GetHandoverNamespaces() map[string]*historyservice.HandoverNamespaceInfo

	// ResolvePendingTaskIDs replaces PendingMaxReplicationTaskID sentinel watermarks
	// with real values. Called when shard state transitions to acquired.
	ResolvePendingTaskIDs(maxReplicationTaskID int64)
}

// HandoverTrackerParams contains the dependencies needed to construct a HandoverTracker.
type HandoverTrackerParams struct {
	ShardID                 int32
	ClusterMetadata         cluster.Metadata
	GetMaxReplicationTaskID func() int64
	ErrorByStateFn          func() error
	NotifyReplicationFn     func(taskID int64)
	NamespaceRegistry       namespace.Registry
	Logger                  log.Logger
	EventLogger             otellog.Logger
}

// HandoverTrackerFactory creates a HandoverTracker.
type HandoverTrackerFactory func(HandoverTrackerParams) HandoverTracker

// defaultHandoverTracker is the OSS implementation keyed by namespace name.
type defaultHandoverTracker struct {
	handoverNamespaces      map[namespace.Name]*namespaceHandOverInfo
	shardID                 int32
	clusterMetadata         cluster.Metadata
	getMaxReplicationTaskID func() int64
	errorByStateFn          func() error
	notifyReplicationFn     func(taskID int64)
	logger                  log.Logger
	eventLogger             otellog.Logger
}

// NewDefaultHandoverTrackerFactory returns a factory that creates the default OSS HandoverTracker.
func NewDefaultHandoverTrackerFactory() HandoverTrackerFactory {
	return func(params HandoverTrackerParams) HandoverTracker {
		return &defaultHandoverTracker{
			handoverNamespaces:      make(map[namespace.Name]*namespaceHandOverInfo),
			shardID:                 params.ShardID,
			clusterMetadata:         params.ClusterMetadata,
			getMaxReplicationTaskID: params.GetMaxReplicationTaskID,
			errorByStateFn:          params.ErrorByStateFn,
			notifyReplicationFn:     params.NotifyReplicationFn,
			logger:                  params.Logger,
			eventLogger:             params.EventLogger,
		}
	}
}

func (t *defaultHandoverTracker) UpdateHandoverState(newNs *namespace.Namespace, deletedFromDB bool) {
	nsName := newNs.Name()
	// NOTE: replication state field won't be replicated and currently we only update a namespace
	// to handover state from active cluster, so the second condition will always be true. Adding
	// it here to be more safe in case above assumption no longer holds in the future.
	isHandoverNamespace := newNs.IsGlobalNamespace() &&
		//nolint:forbidigo // namespace-wide handover tracking; ReplicationState("") below is also ns-level
		newNs.ActiveInCluster(t.clusterMetadata.GetCurrentClusterName()) &&
		newNs.ReplicationState("") == enumspb.REPLICATION_STATE_HANDOVER

	if deletedFromDB || !isHandoverNamespace {
		if removed, ok := t.handoverNamespaces[nsName]; ok {
			delete(t.handoverNamespaces, nsName)
			t.emitWatermarkRemoved(nsName, newNs.ID().String(), removed, deletedFromDB)
		}
		return
	}

	maxReplicationTaskID := t.getMaxReplicationTaskID()
	if t.errorByStateFn() != nil {
		maxReplicationTaskID = PendingMaxReplicationTaskID
	}

	if handover, ok := t.handoverNamespaces[nsName]; ok {
		if handover.NotificationVersion < newNs.NotificationVersion() {
			handover.NotificationVersion = newNs.NotificationVersion()
			handover.MaxReplicationTaskID = maxReplicationTaskID
			t.emitWatermarkSet(nsName, newNs.ID().String(), handover, wideevents.WatermarkUpdated)
		}
	} else {
		handover := &namespaceHandOverInfo{
			NotificationVersion:  newNs.NotificationVersion(),
			MaxReplicationTaskID: maxReplicationTaskID,
		}
		t.handoverNamespaces[nsName] = handover
		t.emitWatermarkSet(nsName, newNs.ID().String(), handover, wideevents.WatermarkAdded)
	}

	if maxReplicationTaskID != PendingMaxReplicationTaskID {
		t.notifyReplicationFn(maxReplicationTaskID)
	}
}

func (t *defaultHandoverTracker) IsInHandover(namespaceName namespace.Name, workflowID string) bool {
	_, ok := t.handoverNamespaces[namespaceName]
	return ok
}

func (t *defaultHandoverTracker) GetHandoverNamespaces() map[string]*historyservice.HandoverNamespaceInfo {
	result := make(map[string]*historyservice.HandoverNamespaceInfo, len(t.handoverNamespaces))
	for k, v := range t.handoverNamespaces {
		result[k.String()] = &historyservice.HandoverNamespaceInfo{
			HandoverReplicationTaskId: v.MaxReplicationTaskID,
		}
	}
	return result
}

func (t *defaultHandoverTracker) ResolvePendingTaskIDs(maxReplicationTaskID int64) {
	for _, handoverInfo := range t.handoverNamespaces {
		if handoverInfo.MaxReplicationTaskID == PendingMaxReplicationTaskID {
			handoverInfo.MaxReplicationTaskID = maxReplicationTaskID
		}
	}
}

// emitWatermarkSet reports this shard taking or advancing its replication watermark for a
// namespace entering handover. pending means the shard was not acquired, so the watermark is the
// sentinel and replication has not been notified yet.
func (t *defaultHandoverTracker) emitWatermarkSet(
	nsName namespace.Name,
	nsID string,
	handover *namespaceHandOverInfo,
	reason string,
) {
	wideevents.Emit(t.eventLogger, wideevents.NamespaceLifecyclePayload{
		Phase:       wideevents.PhaseHandoverWatermarkSet,
		Namespace:   nsName.String(),
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                t.shardID,
			"max_replication_task_id": handover.MaxReplicationTaskID,
			"notification_version":    handover.NotificationVersion,
			"pending":                 handover.MaxReplicationTaskID == PendingMaxReplicationTaskID,
			"reason":                  reason,
		},
	})
}

// emitWatermarkRemoved reports this shard dropping its watermark. deletedFromDB separates a
// namespace deletion from the normal exit out of the handover replication state.
func (t *defaultHandoverTracker) emitWatermarkRemoved(
	nsName namespace.Name,
	nsID string,
	removed *namespaceHandOverInfo,
	deletedFromDB bool,
) {
	wideevents.Emit(t.eventLogger, wideevents.NamespaceLifecyclePayload{
		Phase:       wideevents.PhaseHandoverWatermarkRemoved,
		Namespace:   nsName.String(),
		NamespaceID: nsID,
		Details: map[string]any{
			"shard_id":                t.shardID,
			"max_replication_task_id": removed.MaxReplicationTaskID,
			"notification_version":    removed.NotificationVersion,
			"deleted_from_db":         deletedFromDB,
		},
	})
}
