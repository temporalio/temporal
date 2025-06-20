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
	if len(replicationConfig.Clusters) <= 1 && !replicationClusterListUpdated {
		return nil
	}
	if info.State == enumspb.NAMESPACE_STATE_DELETED {
		// Don't replicate deleted namespace changes.
		return nil
	}

	taskType := enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK
	task := &replicationspb.ReplicationTask_NamespaceTaskAttributes{
		NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
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
			ConfigVersion:   configVersion,
			FailoverVersion: failoverVersion,
			FailoverHistory: convertFailoverHistoryToReplicationProto(failoverHistoy),
		},
	}

	return r.namespaceReplicationQueue.Publish(
		ctx,
		&replicationspb.ReplicationTask{
			TaskType:   taskType,
			Attributes: task,
		})
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
