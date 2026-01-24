//go:generate mockgen -package $GOPACKAGE -source $GOFILE -destination replication_task_handler_mock.go

package nsreplication

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence"
)

var (
	// ErrEmptyNamespaceReplicationTask is the error to indicate empty replication task
	ErrEmptyNamespaceReplicationTask = serviceerror.NewInvalidArgument("empty namespace replication task")
	// ErrInvalidNamespaceOperation is the error to indicate empty namespace operation attribute
	ErrInvalidNamespaceOperation = serviceerror.NewInvalidArgument("invalid namespace operation attribute")
	// ErrInvalidNamespaceID is the error to indicate empty rID attribute
	ErrInvalidNamespaceID = serviceerror.NewInvalidArgument("invalid namespace ID attribute")
	// ErrInvalidNamespaceInfo is the error to indicate empty info attribute
	ErrInvalidNamespaceInfo = serviceerror.NewInvalidArgument("invalid namespace info attribute")
	// ErrInvalidNamespaceConfig is the error to indicate empty config attribute
	ErrInvalidNamespaceConfig = serviceerror.NewInvalidArgument("invalid namespace config attribute")
	// ErrInvalidNamespaceReplicationConfig is the error to indicate empty replication config attribute
	ErrInvalidNamespaceReplicationConfig = serviceerror.NewInvalidArgument("invalid namespace replication config attribute")
	// ErrInvalidNamespaceConfigVersion is the error to indicate empty config version attribute
	ErrInvalidNamespaceConfigVersion = serviceerror.NewInvalidArgument("invalid namespace config version attribute")
	// ErrInvalidNamespaceFailoverVersion is the error to indicate empty failover version attribute
	ErrInvalidNamespaceFailoverVersion = serviceerror.NewInvalidArgument("invalid namespace failover version attribute")
	// ErrInvalidNamespaceState is the error to indicate invalid namespace state
	ErrInvalidNamespaceState = serviceerror.NewInvalidArgument("invalid namespace state attribute")
	// ErrNameUUIDCollision is the error to indicate namespace name / UUID collision
	ErrNameUUIDCollision = serviceerror.NewInvalidArgument("namespace replication encountered name / UUID collision")
)

// NOTE: the counterpart of namespace replication transmission logic is in service/frontend package

type (
	// TaskExecutor is the interface for executing namespace replication tasks
	TaskExecutor interface {
		Execute(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) error
	}

	taskExecutorImpl struct {
		currentCluster  string
		metadataManager persistence.MetadataManager
		logger          log.Logger
	}
)

// NewTaskExecutor creates a new instance of namespace replicator
func NewTaskExecutor(
	currentCluster string,
	metadataManagerV2 persistence.MetadataManager,
	logger log.Logger,
) TaskExecutor {

	return &taskExecutorImpl{
		currentCluster:  currentCluster,
		metadataManager: metadataManagerV2,
		logger:          logger,
	}
}

// Execute handles receiving of the namespace replication task
func (h *taskExecutorImpl) Execute(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	if err := h.validateNamespaceReplicationTask(task); err != nil {
		return err
	}
	if shouldProcess, err := h.shouldProcessTask(ctx, task); !shouldProcess || err != nil {
		return err
	}

	switch task.GetNamespaceOperation() {
	case enumsspb.NAMESPACE_OPERATION_CREATE:
		return h.handleNamespaceCreationReplicationTask(ctx, task)
	case enumsspb.NAMESPACE_OPERATION_UPDATE:
		return h.handleNamespaceUpdateReplicationTask(ctx, task)
	default:
		return ErrInvalidNamespaceOperation
	}
}

func checkClusterIncludedInReplicationConfig(clusterName string, repCfg []*replicationpb.ClusterReplicationConfig) bool {
	for _, cluster := range repCfg {
		if clusterName == cluster.GetClusterName() {
			return true
		}
	}
	return false
}

func (h *taskExecutorImpl) shouldProcessTask(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (bool, error) {
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.GetInfo().GetName(),
	})
	switch err.(type) {
	case nil:
		if resp.Namespace.GetInfo().GetId() != task.GetId() {
			h.logger.Error(
				"namespace replication encountered UUID collision processing namespace replication task",
				tag.WorkflowNamespaceID(resp.Namespace.GetInfo().GetId()),
				tag.NewStringTag("Task Namespace Id", task.GetId()),
				tag.NewStringTag("Task Namespace Info Id", task.GetInfo().GetId()))
			return false, ErrNameUUIDCollision
		}

		return true, nil
	case *serviceerror.NamespaceNotFound:
		return checkClusterIncludedInReplicationConfig(h.currentCluster, task.GetReplicationConfig().GetClusters()), nil
	default:
		// return the original err
		return false, err
	}
}

// handleNamespaceCreationReplicationTask handles the namespace creation replication task
func (h *taskExecutorImpl) handleNamespaceCreationReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	// task already validated
	err := h.validateNamespaceStatus(task.GetInfo().GetState())
	if err != nil {
		return err
	}

	request := &persistence.CreateNamespaceRequest{
		Namespace: persistencespb.NamespaceDetail_builder{
			Info: persistencespb.NamespaceInfo_builder{
				Id:          task.GetId(),
				Name:        task.GetInfo().GetName(),
				State:       task.GetInfo().GetState(),
				Description: task.GetInfo().GetDescription(),
				Owner:       task.GetInfo().GetOwnerEmail(),
				Data:        task.GetInfo().GetData(),
			}.Build(),
			Config: persistencespb.NamespaceConfig_builder{
				Retention:                    task.GetConfig().GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:         task.GetConfig().GetHistoryArchivalState(),
				HistoryArchivalUri:           task.GetConfig().GetHistoryArchivalUri(),
				VisibilityArchivalState:      task.GetConfig().GetVisibilityArchivalState(),
				VisibilityArchivalUri:        task.GetConfig().GetVisibilityArchivalUri(),
				CustomSearchAttributeAliases: task.GetConfig().GetCustomSearchAttributeAliases(),
			}.Build(),
			ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: task.GetReplicationConfig().GetActiveClusterName(),
				Clusters:          ConvertClusterReplicationConfigFromProto(task.GetReplicationConfig().GetClusters()),
				FailoverHistory:   ConvertFailoverHistoryToPersistenceProto(task.GetFailoverHistory()),
			}.Build(),
			ConfigVersion:   task.GetConfigVersion(),
			FailoverVersion: task.GetFailoverVersion(),
		}.Build(),
		IsGlobalNamespace: true, // local namespace will not be replicated
	}

	_, err = h.metadataManager.CreateNamespace(ctx, request)
	if err != nil {
		// SQL and Cassandra handle namespace UUID collision differently
		// here, whenever seeing a error replicating a namespace
		// do a check if there is a name / UUID collision

		recordExists := true
		resp, getErr := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			Name: task.GetInfo().GetName(),
		})
		switch getErr.(type) {
		case nil:
			if resp.Namespace.GetInfo().GetId() != task.GetId() {
				h.logger.Error("namespace replication encountered UUID collision during NamespaceCreationReplicationTask",
					tag.WorkflowNamespaceID(resp.Namespace.GetInfo().GetId()),
					tag.NewStringTag("Task Namespace Id", task.GetId()),
					tag.NewStringTag("Task Namespace Info Id", task.GetInfo().GetId()),
					tag.Error(err))
				return ErrNameUUIDCollision
			}
		case *serviceerror.NamespaceNotFound:
			// no check is necessary
			recordExists = false
		default:
			// return the original err
			h.logger.Error(
				"namespace replication encountered error during NamespaceCreationReplicationTask",
				tag.WorkflowNamespace(task.GetInfo().GetName()),
				tag.WorkflowNamespaceID(task.GetInfo().GetId()),
				tag.Error(err))
			return err
		}

		resp, getErr = h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
			ID: task.GetId(),
		})
		switch getErr.(type) {
		case nil:
			if resp.Namespace.GetInfo().GetName() != task.GetInfo().GetName() {
				h.logger.Error(
					"namespace replication encountered name collision during NamespaceCreationReplicationTask",
					tag.WorkflowNamespace(resp.Namespace.GetInfo().GetName()),
					tag.NewStringTag("Task Namespace Name", task.GetInfo().GetName()),
					tag.Error(err))
				return ErrNameUUIDCollision
			}
		case *serviceerror.NamespaceNotFound:
			// no check is necessary
			recordExists = false
		default:
			// return the original err
			return err
		}

		if recordExists {
			// name -> id & id -> name check pass, this is duplication request
			return nil
		}
		return err
	}

	return err
}

// handleNamespaceUpdateReplicationTask handles the namespace update replication task
func (h *taskExecutorImpl) handleNamespaceUpdateReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	// task already validated
	err := h.validateNamespaceStatus(task.GetInfo().GetState())
	if err != nil {
		return err
	}

	// first we need to get the current notification version since we need to it for conditional update
	metadata, err := h.metadataManager.GetMetadata(ctx)
	if err != nil {
		return err
	}
	notificationVersion := metadata.NotificationVersion

	// plus, we need to check whether the config version is <= the config version set in the input
	// plus, we need to check whether the failover version is <= the failover version set in the input
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.GetInfo().GetName(),
	})
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// this can happen if the create namespace replication task is to processed.
			// e.g. new cluster which does not have anything
			return h.handleNamespaceCreationReplicationTask(ctx, task)
		}
		return err
	}

	recordUpdated := false
	request := &persistence.UpdateNamespaceRequest{
		Namespace:           resp.Namespace,
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   resp.IsGlobalNamespace,
	}

	if resp.Namespace.GetConfigVersion() < task.GetConfigVersion() {
		recordUpdated = true
		request.Namespace.SetInfo(persistencespb.NamespaceInfo_builder{
			Id:          task.GetId(),
			Name:        task.GetInfo().GetName(),
			State:       task.GetInfo().GetState(),
			Description: task.GetInfo().GetDescription(),
			Owner:       task.GetInfo().GetOwnerEmail(),
			Data:        task.GetInfo().GetData(),
		}.Build())
		request.Namespace.SetConfig(persistencespb.NamespaceConfig_builder{
			Retention:                    task.GetConfig().GetWorkflowExecutionRetentionTtl(),
			HistoryArchivalState:         task.GetConfig().GetHistoryArchivalState(),
			HistoryArchivalUri:           task.GetConfig().GetHistoryArchivalUri(),
			VisibilityArchivalState:      task.GetConfig().GetVisibilityArchivalState(),
			VisibilityArchivalUri:        task.GetConfig().GetVisibilityArchivalUri(),
			CustomSearchAttributeAliases: task.GetConfig().GetCustomSearchAttributeAliases(),
		}.Build())
		if task.GetConfig().GetBadBinaries() != nil {
			request.Namespace.GetConfig().SetBadBinaries(task.GetConfig().GetBadBinaries())
		}
		request.Namespace.GetReplicationConfig().SetClusters(ConvertClusterReplicationConfigFromProto(task.GetReplicationConfig().GetClusters()))
		request.Namespace.SetConfigVersion(task.GetConfigVersion())
	}
	if resp.Namespace.GetFailoverVersion() < task.GetFailoverVersion() {
		recordUpdated = true
		request.Namespace.GetReplicationConfig().SetActiveClusterName(task.GetReplicationConfig().GetActiveClusterName())
		request.Namespace.SetFailoverVersion(task.GetFailoverVersion())
		request.Namespace.SetFailoverNotificationVersion(notificationVersion)
		request.Namespace.GetReplicationConfig().SetFailoverHistory(ConvertFailoverHistoryToPersistenceProto(task.GetFailoverHistory()))
	}

	if !recordUpdated {
		return nil
	}

	return h.metadataManager.UpdateNamespace(ctx, request)
}

func (h *taskExecutorImpl) validateNamespaceReplicationTask(task *replicationspb.NamespaceTaskAttributes) error {
	if task == nil {
		return ErrEmptyNamespaceReplicationTask
	}

	if task.GetId() == "" {
		return ErrInvalidNamespaceID
	} else if !task.HasInfo() {
		return ErrInvalidNamespaceInfo
	} else if !task.HasConfig() {
		return ErrInvalidNamespaceConfig
	} else if !task.HasReplicationConfig() {
		return ErrInvalidNamespaceReplicationConfig
	}
	return nil
}

func ConvertClusterReplicationConfigFromProto(
	input []*replicationpb.ClusterReplicationConfig,
) []string {
	var output []string
	for _, cluster := range input {
		clusterName := cluster.GetClusterName()
		output = append(output, clusterName)
	}
	return output
}

func ConvertFailoverHistoryToPersistenceProto(failoverHistory []*replicationpb.FailoverStatus) []*persistencespb.FailoverStatus {
	var res []*persistencespb.FailoverStatus
	for _, status := range failoverHistory {
		res = append(res, persistencespb.FailoverStatus_builder{
			FailoverTime:    status.GetFailoverTime(),
			FailoverVersion: status.GetFailoverVersion(),
		}.Build())
	}
	return res
}

func (h *taskExecutorImpl) validateNamespaceStatus(input enumspb.NamespaceState) error {
	switch input {
	case enumspb.NAMESPACE_STATE_REGISTERED, enumspb.NAMESPACE_STATE_DEPRECATED:
		return nil
	default:
		return ErrInvalidNamespaceState
	}
}
