package nsreplication

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

// NOTE: the counterpart of namespace replication receiving logic is in service/worker package

type (
	// Replicator is the interface which can replicate the namespace
	Replicator interface {
		HandleTransmissionTask(
			ctx context.Context,
			namespaceOperation enumsspb.NamespaceOperation,
			info *persistencespb.NamespaceInfo,
			config *persistencespb.NamespaceConfig,
			replicationConfig *persistencespb.NamespaceReplicationConfig,
			replicationClusterListUpdated bool,
			configVersion int64,
			failoverVersion int64,
			isGlobalNamespace bool,
			failoverHistoy []*persistencespb.FailoverStatus,
			forceReplicate bool,
		) error
	}

	replicator struct {
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		logger                    log.Logger
	}
)

// NewReplicator create a new instance of namespace replicator
func NewReplicator(
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	logger log.Logger,
) Replicator {
	return &replicator{
		namespaceReplicationQueue: namespaceReplicationQueue,
		logger:                    logger,
	}
}

// HandleTransmissionTask handle transmission of the namespace replication task
func (r *replicator) HandleTransmissionTask(
	ctx context.Context,
	namespaceOperation enumsspb.NamespaceOperation,
	info *persistencespb.NamespaceInfo,
	config *persistencespb.NamespaceConfig,
	replicationConfig *persistencespb.NamespaceReplicationConfig,
	replicationClusterListUpdated bool,
	configVersion int64,
	failoverVersion int64,
	isGlobalNamespace bool,
	failoverHistoy []*persistencespb.FailoverStatus,
	forceReplicate bool,
) error {

	if !ShouldReplicateNamespace(
		forceReplicate,
		isGlobalNamespace,
		replicationConfig.Clusters,
		replicationClusterListUpdated,
		info.State,
	) {
		return nil
	}

	// Build the wire payload through the shared converter. Extracting this build
	// step is groundwork for an eventual CHASM-based namespace replication
	// transport: when that path is added it will build its requests through this
	// same converter, so the detail->wire conversion can never diverge between the
	// two transports. Today only this legacy queue path calls it. FailoverHistory
	// is threaded in explicitly because callers pass it separately from
	// replicationConfig.
	detail := &persistencespb.NamespaceDetail{
		Info:   info,
		Config: config,
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			State:             replicationConfig.State,
			Clusters:          replicationConfig.Clusters,
			FailoverHistory:   failoverHistoy,
		},
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
	}

	return r.namespaceReplicationQueue.Publish(
		ctx,
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: NamespaceDetailToTaskAttributes(namespaceOperation, detail),
			},
		})
}

// ShouldReplicateNamespace reports whether a namespace mutation must be
// propagated to peer clusters at all. It is the single replicate/skip gate,
// extracted from HandleTransmissionTask in preparation for an eventual
// CHASM-based namespace replication transport: that path will share this exact
// decision so the two transports can never diverge on which mutations replicate.
// Today only the legacy queue path (HandleTransmissionTask) calls it.
//
// The entire force/deleted/global/peer decision lives here, in one place, so no
// caller can accidentally bypass part of it:
//
//   - A DELETED namespace is never replicated, even under forceReplicate:
//     namespace deletion is coordinated through a separate path and must never be
//     pushed to peers. This is checked first, ahead of forceReplicate, so force
//     cannot bypass it.
//   - forceReplicate then replicates unconditionally (a non-deleted namespace).
//   - Otherwise a mutation replicates only when the namespace is global and has a
//     peer to replicate to (more than one cluster, or the cluster list just
//     changed).
func ShouldReplicateNamespace(
	forceReplicate bool,
	isGlobalNamespace bool,
	clusters []string,
	replicationClusterListUpdated bool,
	state enumspb.NamespaceState,
) bool {
	if state == enumspb.NAMESPACE_STATE_DELETED {
		return false
	}
	if forceReplicate {
		return true
	}
	if !isGlobalNamespace {
		return false
	}
	if len(clusters) <= 1 && !replicationClusterListUpdated {
		return false
	}
	return true
}

// NamespaceDetailToTaskAttributes converts a namespace detail into the
// NamespaceTaskAttributes wire shape consumed by the receiver-side
// apply-if-higher logic (TaskExecutor).
//
// It is extracted here as the single source of truth for that conversion in
// preparation for an eventual CHASM-based namespace replication transport: when
// that path is added it will build its outbound requests through this same
// function, so a replicated field can never be emitted by one transport and
// silently dropped by the other. Today only the legacy queue path
// (HandleTransmissionTask) calls it; the extraction itself is a pure
// no-behavior-change refactor.
func NamespaceDetailToTaskAttributes(
	namespaceOperation enumsspb.NamespaceOperation,
	detail *persistencespb.NamespaceDetail,
) *replicationspb.NamespaceTaskAttributes {
	info := detail.Info
	config := detail.Config
	replicationConfig := detail.ReplicationConfig

	attributes := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: namespaceOperation,
		Id:                 info.Id,
		Info: &namespacepb.NamespaceInfo{
			Name:        info.Name,
			State:       info.State,
			Description: info.Description,
			OwnerEmail:  info.Owner,
			Data:        info.Data,
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: config.Retention,
			HistoryArchivalState:          config.HistoryArchivalState,
			HistoryArchivalUri:            config.HistoryArchivalUri,
			VisibilityArchivalState:       config.VisibilityArchivalState,
			VisibilityArchivalUri:         config.VisibilityArchivalUri,
			BadBinaries:                   config.BadBinaries,
			CustomSearchAttributeAliases:  config.CustomSearchAttributeAliases,
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.ActiveClusterName,
			Clusters:          convertClusterReplicationConfigToProto(replicationConfig.Clusters),
		},
		ConfigVersion:   detail.ConfigVersion,
		FailoverVersion: detail.FailoverVersion,
		FailoverHistory: convertFailoverHistoryToReplicationProto(replicationConfig.FailoverHistory),
	}

	if replicationConfig.State == enumspb.REPLICATION_STATE_NORMAL {
		attributes.ReplicationConfig.State = replicationConfig.State
	}
	return attributes
}

func convertClusterReplicationConfigToProto(
	input []string,
) []*replicationpb.ClusterReplicationConfig {
	output := make([]*replicationpb.ClusterReplicationConfig, 0, len(input))
	for _, clusterName := range input {
		output = append(output, &replicationpb.ClusterReplicationConfig{ClusterName: clusterName})
	}
	return output
}

func convertFailoverHistoryToReplicationProto(
	failoverHistoy []*persistencespb.FailoverStatus,
) []*replicationpb.FailoverStatus {
	var replicationProto []*replicationpb.FailoverStatus
	for _, failoverStatus := range failoverHistoy {
		replicationProto = append(replicationProto, &replicationpb.FailoverStatus{
			FailoverTime:    failoverStatus.GetFailoverTime(),
			FailoverVersion: failoverStatus.GetFailoverVersion(),
		})
	}

	return replicationProto
}
