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
) error {

	if !isGlobalNamespace {
		return nil
	}
	if len(replicationConfig.GetClusters()) <= 1 && !replicationClusterListUpdated {
		return nil
	}
	if info.GetState() == enumspb.NAMESPACE_STATE_DELETED {
		// Don't replicate deleted namespace changes.
		return nil
	}

	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	taskAttrs := replicationspb.NamespaceTaskAttributes_builder{
		NamespaceOperation: namespaceOperation,
		Id:                 info.GetId(),
		Info: namespacepb.NamespaceInfo_builder{
			Name:        info.GetName(),
			State:       info.GetState(),
			Description: info.GetDescription(),
			OwnerEmail:  info.GetOwner(),
			Data:        info.GetData(),
		}.Build(),
		Config: namespacepb.NamespaceConfig_builder{
			WorkflowExecutionRetentionTtl: config.GetRetention(),
			HistoryArchivalState:          config.GetHistoryArchivalState(),
			HistoryArchivalUri:            config.GetHistoryArchivalUri(),
			VisibilityArchivalState:       config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:         config.GetVisibilityArchivalUri(),
			BadBinaries:                   config.GetBadBinaries(),
			CustomSearchAttributeAliases:  config.GetCustomSearchAttributeAliases(),
		}.Build(),
		ReplicationConfig: replicationpb.NamespaceReplicationConfig_builder{
			ActiveClusterName: replicationConfig.GetActiveClusterName(),
			Clusters:          convertClusterReplicationConfigToProto(replicationConfig.GetClusters()),
		}.Build(),
		ConfigVersion:   configVersion,
		FailoverVersion: failoverVersion,
		FailoverHistory: convertFailoverHistoryToReplicationProto(failoverHistoy),
	}.Build()

	return r.namespaceReplicationQueue.Publish(
		ctx,
		replicationspb.ReplicationTask_builder{
			TaskType:                taskType,
			NamespaceTaskAttributes: taskAttrs,
		}.Build())
}

func convertClusterReplicationConfigToProto(
	input []string,
) []*replicationpb.ClusterReplicationConfig {
	output := make([]*replicationpb.ClusterReplicationConfig, 0, len(input))
	for _, clusterName := range input {
		output = append(output, replicationpb.ClusterReplicationConfig_builder{ClusterName: clusterName}.Build())
	}
	return output
}

func convertFailoverHistoryToReplicationProto(
	failoverHistoy []*persistencespb.FailoverStatus,
) []*replicationpb.FailoverStatus {
	var replicationProto []*replicationpb.FailoverStatus
	for _, failoverStatus := range failoverHistoy {
		replicationProto = append(replicationProto, replicationpb.FailoverStatus_builder{
			FailoverTime:    failoverStatus.GetFailoverTime(),
			FailoverVersion: failoverStatus.GetFailoverVersion(),
		}.Build())
	}

	return replicationProto
}
