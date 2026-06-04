package shard

import (
	"math"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
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
	ClusterMetadata         cluster.Metadata
	GetMaxReplicationTaskID func() int64
	ErrorByStateFn          func() error
	NotifyReplicationFn     func(taskID int64)
	NamespaceRegistry       namespace.Registry
	Logger                  log.Logger
}

// HandoverTrackerFactory creates a HandoverTracker.
type HandoverTrackerFactory func(HandoverTrackerParams) HandoverTracker

// defaultHandoverTracker is the OSS implementation keyed by namespace name.
type defaultHandoverTracker struct {
	handoverNamespaces      map[namespace.Name]*namespaceHandOverInfo
	clusterMetadata         cluster.Metadata
	getMaxReplicationTaskID func() int64
	errorByStateFn          func() error
	notifyReplicationFn     func(taskID int64)
	logger                  log.Logger
}

// NewDefaultHandoverTrackerFactory returns a factory that creates the default OSS HandoverTracker.
func NewDefaultHandoverTrackerFactory() HandoverTrackerFactory {
	return func(params HandoverTrackerParams) HandoverTracker {
		return &defaultHandoverTracker{
			handoverNamespaces:      make(map[namespace.Name]*namespaceHandOverInfo),
			clusterMetadata:         params.ClusterMetadata,
			getMaxReplicationTaskID: params.GetMaxReplicationTaskID,
			errorByStateFn:          params.ErrorByStateFn,
			notifyReplicationFn:     params.NotifyReplicationFn,
			logger:                  params.Logger,
		}
	}
}

func (t *defaultHandoverTracker) UpdateHandoverState(newNs *namespace.Namespace, deletedFromDB bool) {
	nsName := newNs.Name()
	// NOTE: replication state field won't be replicated and currently we only update a namespace
	// to handover state from active cluster, so the second condition will always be true. Adding
	// it here to be more safe in case above assumption no longer holds in the future.
	isHandoverNamespace := newNs.IsGlobalNamespace() &&
		newNs.ActiveInCluster(t.clusterMetadata.GetCurrentClusterName()) &&
		newNs.ReplicationState("") == enumspb.REPLICATION_STATE_HANDOVER

	if deletedFromDB || !isHandoverNamespace {
		delete(t.handoverNamespaces, nsName)
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
		}
	} else {
		t.handoverNamespaces[nsName] = &namespaceHandOverInfo{
			NotificationVersion:  newNs.NotificationVersion(),
			MaxReplicationTaskID: maxReplicationTaskID,
		}
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
