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

	if info.State == enumspb.NAMESPACE_STATE_DELETED {
		// Deleted namespaces are never replicated through this path (even under
		// forceReplicate); namespace deletion is coordinated separately.
		return nil
	}
	if !forceReplicate && !ShouldReplicateNamespace(
		isGlobalNamespace,
		replicationConfig.GetClusters(),
		replicationClusterListUpdated,
		info.State,
	) {
		return nil
	}

	// Build the wire payload through the shared converter so this queue path and
	// the CHASM push path can never diverge on the shape they emit. FailoverHistory
	// is threaded in explicitly because callers pass it separately from replicationConfig.
	detail := &persistencespb.NamespaceDetail{
		Info:   info,
		Config: config,
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.GetActiveClusterName(),
			State:             replicationConfig.GetState(),
			Clusters:          replicationConfig.GetClusters(),
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
// propagated to peer clusters at all. It is the single gate shared by both
// replication transports — the legacy queue (HandleTransmissionTask) and the
// CHASM-based push invoked from the frontend — so the two paths cannot diverge
// on which mutations replicate.
//
// A mutation replicates only when the namespace is global, has a peer to
// replicate to (more than one cluster, or the cluster list just changed), and
// is not being deleted (namespace deletion is coordinated through a separate
// path and must never be pushed to peers).
func ShouldReplicateNamespace(
	isGlobalNamespace bool,
	clusters []string,
	replicationClusterListUpdated bool,
	state enumspb.NamespaceState,
) bool {
	if state == enumspb.NAMESPACE_STATE_DELETED {
		return false
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
// apply-if-higher logic (TaskExecutor). It is the single source of truth for
// that conversion, shared by the legacy queue transport (HandleTransmissionTask)
// and the CHASM push transport, so a field can never be replicated by one path
// and silently dropped by the other.
func NamespaceDetailToTaskAttributes(
	namespaceOperation enumsspb.NamespaceOperation,
	detail *persistencespb.NamespaceDetail,
) *replicationspb.NamespaceTaskAttributes {
	info := detail.GetInfo()
	config := detail.GetConfig()
	replicationConfig := detail.GetReplicationConfig()

	attributes := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: namespaceOperation,
		Id:                 info.GetId(),
		Info: &namespacepb.NamespaceInfo{
			Name:        info.GetName(),
			State:       info.GetState(),
			Description: info.GetDescription(),
			OwnerEmail:  info.GetOwner(),
			Data:        info.GetData(),
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: config.GetRetention(),
			HistoryArchivalState:          config.GetHistoryArchivalState(),
			HistoryArchivalUri:            config.GetHistoryArchivalUri(),
			VisibilityArchivalState:       config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:         config.GetVisibilityArchivalUri(),
			BadBinaries:                   config.GetBadBinaries(),
			CustomSearchAttributeAliases:  config.GetCustomSearchAttributeAliases(),
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: replicationConfig.GetActiveClusterName(),
			Clusters:          convertClusterReplicationConfigToProto(replicationConfig.GetClusters()),
		},
		ConfigVersion:   detail.GetConfigVersion(),
		FailoverVersion: detail.GetFailoverVersion(),
		FailoverHistory: convertFailoverHistoryToReplicationProto(replicationConfig.GetFailoverHistory()),
	}

	if replicationConfig.GetState() == enumspb.REPLICATION_STATE_NORMAL {
		attributes.ReplicationConfig.State = replicationConfig.GetState()
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
