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
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
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

// ApplyOutcome reports what happened when a namespace replication task was
// applied to the local metadata store. The queue-based path discards this and
// only looks at the error; the CHASM-based admin RPC path surfaces it on the
// wire so callers can distinguish "we wrote new state" from "we already had
// equal-or-newer state" for observability and metrics.
type ApplyOutcome int

const (
	// OutcomeUnspecified is the zero value. Returned alongside any non-nil error.
	OutcomeUnspecified ApplyOutcome = iota
	// OutcomeApplied means an existing namespace was updated (incoming ConfigVersion
	// or FailoverVersion was strictly higher than the receiver's current values).
	OutcomeApplied
	// OutcomeCreated means the namespace did not exist on this cell and was created.
	OutcomeCreated
	// OutcomeNoOpStale means the incoming mutation was not newer than the
	// receiver's current state and no write was performed. Not a failure.
	OutcomeNoOpStale
	// OutcomeDuplicate means CreateNamespace failed because a matching record
	// already existed (same id and name). The create is treated as success.
	OutcomeDuplicate
	// OutcomeNotAdmitted means the admitter rejected the namespace. No write.
	OutcomeNotAdmitted
)

type (
	// TaskExecutor is the interface for executing namespace replication tasks.
	TaskExecutor interface {
		// Execute applies the task. Returns nil on any success (Applied, Created,
		// NoOpStale, Duplicate, NotAdmitted) or an error on failure. Used by the
		// queue-based replication path, which doesn't care about the outcome.
		Execute(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) error

		// ExecuteWithOutcome is Execute but also reports which outcome occurred.
		// Used by the CHASM-based admin RPC path to surface the outcome on the wire.
		ExecuteWithOutcome(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (ApplyOutcome, error)
	}

	taskExecutorImpl struct {
		currentCluster  string
		metadataManager persistence.MetadataManager
		dataMerger      NamespaceDataMerger
		admitter        NamespaceReplicationAdmitter
		logger          log.Logger
		testHooks       testhooks.TestHooks
	}
)

// NewTaskExecutor creates a new instance of namespace replicator
func NewTaskExecutor(
	currentCluster string,
	metadataManagerV2 persistence.MetadataManager,
	dataMerger NamespaceDataMerger,
	admitter NamespaceReplicationAdmitter,
	logger log.Logger,
	testHooks testhooks.TestHooks,
) TaskExecutor {
	return &taskExecutorImpl{
		currentCluster:  currentCluster,
		metadataManager: metadataManagerV2,
		dataMerger:      dataMerger,
		admitter:        admitter,
		logger:          logger,
		testHooks:       testHooks,
	}
}

// Execute handles receiving of the namespace replication task. Used by the
// queue-based path, which only cares about success vs failure. Outcome details
// are discarded; callers that want them should use [ExecuteWithOutcome].
func (h *taskExecutorImpl) Execute(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) error {
	_, err := h.ExecuteWithOutcome(ctx, task)
	return err
}

// ExecuteWithOutcome handles receiving of the namespace replication task and
// reports the outcome (Applied / Created / NoOpStale / Duplicate / NotAdmitted).
// Used by the CHASM-based admin RPC path so the wire response can distinguish
// "we wrote new state" from "we already had equal-or-newer state."
func (h *taskExecutorImpl) ExecuteWithOutcome(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) (ApplyOutcome, error) {
	if err := h.validateNamespaceReplicationTask(task); err != nil {
		return OutcomeUnspecified, err
	}
	if hook, ok := testhooks.Get(
		h.testHooks,
		testhooks.NamespaceReplicationTaskInterceptor,
		namespace.Name(task.GetInfo().GetName()),
	); ok {
		// The hook signature is fixed (returns error). Close over outcome so we
		// can still report it while keeping the hook contract intact.
		var outcome ApplyOutcome
		err := hook(ctx, task, func() error {
			var inner error
			outcome, inner = h.executeValidatedTask(ctx, task)
			return inner
		})
		return outcome, err
	}
	return h.executeValidatedTask(ctx, task)
}

func (h *taskExecutorImpl) executeValidatedTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) (ApplyOutcome, error) {
	if shouldProcess, err := h.shouldProcessTask(ctx, task); !shouldProcess || err != nil {
		if err != nil {
			return OutcomeUnspecified, err
		}
		// shouldProcess == false && err == nil: admitter rejected the task.
		return OutcomeNotAdmitted, nil
	}

	switch task.GetNamespaceOperation() {
	case enumsspb.NAMESPACE_OPERATION_CREATE:
		return h.handleNamespaceCreationReplicationTask(ctx, task)
	case enumsspb.NAMESPACE_OPERATION_UPDATE:
		return h.handleNamespaceUpdateReplicationTask(ctx, task)
	default:
		return OutcomeUnspecified, ErrInvalidNamespaceOperation
	}
}

func (h *taskExecutorImpl) shouldProcessTask(ctx context.Context, task *replicationspb.NamespaceTaskAttributes) (bool, error) {
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.Info.GetName(),
	})
	switch err.(type) {
	case nil:
		if resp.Namespace.Info.Id != task.GetId() {
			h.logger.Error(
				"namespace replication encountered UUID collision processing namespace replication task",
				tag.WorkflowNamespaceID(resp.Namespace.Info.Id),
				tag.String("Task Namespace Id", task.GetId()),
				tag.String("Task Namespace Info Id", task.Info.GetId()))
			return false, ErrNameUUIDCollision
		}

		return true, nil
	case *serviceerror.NamespaceNotFound:
		return h.admitter.Admit(ctx, h.currentCluster, task), nil
	default:
		// return the original err
		return false, err
	}
}

// handleNamespaceCreationReplicationTask handles the namespace creation replication task
func (h *taskExecutorImpl) handleNamespaceCreationReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) (ApplyOutcome, error) {
	// task already validated
	err := h.validateNamespaceStatus(task.Info.State)
	if err != nil {
		return OutcomeUnspecified, err
	}

	request := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:          task.GetId(),
				Name:        task.Info.GetName(),
				State:       task.Info.GetState(),
				Description: task.Info.GetDescription(),
				Owner:       task.Info.GetOwnerEmail(),
				Data:        task.Info.Data,
			},
			Config: &persistencespb.NamespaceConfig{
				Retention:                    task.Config.GetWorkflowExecutionRetentionTtl(),
				HistoryArchivalState:         task.Config.GetHistoryArchivalState(),
				HistoryArchivalUri:           task.Config.GetHistoryArchivalUri(),
				VisibilityArchivalState:      task.Config.GetVisibilityArchivalState(),
				VisibilityArchivalUri:        task.Config.GetVisibilityArchivalUri(),
				CustomSearchAttributeAliases: task.Config.GetCustomSearchAttributeAliases(),
			},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: task.ReplicationConfig.GetActiveClusterName(),
				Clusters:          ConvertClusterReplicationConfigFromProto(task.ReplicationConfig.Clusters),
				State:             task.ReplicationConfig.GetState(),
				FailoverHistory:   ConvertFailoverHistoryToPersistenceProto(task.GetFailoverHistory()),
			},
			ConfigVersion:   task.GetConfigVersion(),
			FailoverVersion: task.GetFailoverVersion(),
		},
		IsGlobalNamespace: true, // local namespace will not be replicated
	}

	_, err = h.metadataManager.CreateNamespace(ctx, request)
	if err == nil {
		return OutcomeCreated, nil
	}

	// SQL and Cassandra handle namespace UUID collision differently
	// here, whenever seeing a error replicating a namespace
	// do a check if there is a name / UUID collision

	recordExists := true
	resp, getErr := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.Info.GetName(),
	})
	switch getErr.(type) {
	case nil:
		if resp.Namespace.Info.Id != task.GetId() {
			h.logger.Error("namespace replication encountered UUID collision during NamespaceCreationReplicationTask",
				tag.WorkflowNamespaceID(resp.Namespace.Info.Id),
				tag.String("Task Namespace Id", task.GetId()),
				tag.String("Task Namespace Info Id", task.Info.GetId()),
				tag.Error(err))
			return OutcomeUnspecified, ErrNameUUIDCollision
		}
	case *serviceerror.NamespaceNotFound:
		// no check is necessary
		recordExists = false
	default:
		// return the original err
		h.logger.Error(
			"namespace replication encountered error during NamespaceCreationReplicationTask",
			tag.WorkflowNamespace(task.Info.GetName()),
			tag.WorkflowNamespaceID(task.Info.GetId()),
			tag.Error(err))
		return OutcomeUnspecified, err
	}

	resp, getErr = h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		ID: task.GetId(),
	})
	switch getErr.(type) {
	case nil:
		if resp.Namespace.Info.Name != task.Info.GetName() {
			h.logger.Error(
				"namespace replication encountered name collision during NamespaceCreationReplicationTask",
				tag.WorkflowNamespace(resp.Namespace.Info.Name),
				tag.String("Task Namespace Name", task.Info.GetName()),
				tag.Error(err))
			return OutcomeUnspecified, ErrNameUUIDCollision
		}
	case *serviceerror.NamespaceNotFound:
		// no check is necessary
		recordExists = false
	default:
		// return the original err
		return OutcomeUnspecified, err
	}

	if recordExists {
		// name -> id & id -> name check pass, this is duplication request
		return OutcomeDuplicate, nil
	}
	return OutcomeUnspecified, err
}

// handleNamespaceUpdateReplicationTask handles the namespace update replication task
func (h *taskExecutorImpl) handleNamespaceUpdateReplicationTask(
	ctx context.Context,
	task *replicationspb.NamespaceTaskAttributes,
) (ApplyOutcome, error) {
	// task already validated
	err := h.validateNamespaceStatus(task.Info.State)
	if err != nil {
		return OutcomeUnspecified, err
	}

	// first we need to get the current notification version since we need to it for conditional update
	metadata, err := h.metadataManager.GetMetadata(ctx)
	if err != nil {
		return OutcomeUnspecified, err
	}
	notificationVersion := metadata.NotificationVersion

	// plus, we need to check whether the config version is <= the config version set in the input
	// plus, we need to check whether the failover version is <= the failover version set in the input
	resp, err := h.metadataManager.GetNamespace(ctx, &persistence.GetNamespaceRequest{
		Name: task.Info.GetName(),
	})
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NamespaceNotFound); isNotFound {
			// this can happen if the create namespace replication task is to processed.
			// e.g. new cluster which does not have anything
			return h.handleNamespaceCreationReplicationTask(ctx, task)
		}
		return OutcomeUnspecified, err
	}

	recordUpdated := false
	request := &persistence.UpdateNamespaceRequest{
		Namespace:           resp.Namespace,
		NotificationVersion: notificationVersion,
		IsGlobalNamespace:   resp.IsGlobalNamespace,
	}

	mergedData, dataMerged := h.dataMerger.MergeData(resp.Namespace.Info.Data, task.Info.Data)
	if dataMerged {
		recordUpdated = true
		request.Namespace.Info.Data = mergedData
	}

	if resp.Namespace.ConfigVersion < task.GetConfigVersion() {
		recordUpdated = true
		// Use merged data if available, otherwise use task data
		data := task.Info.Data
		if dataMerged {
			data = mergedData
		}
		request.Namespace.Info = &persistencespb.NamespaceInfo{
			Id:          task.GetId(),
			Name:        task.Info.GetName(),
			State:       task.Info.GetState(),
			Description: task.Info.GetDescription(),
			Owner:       task.Info.GetOwnerEmail(),
			Data:        data,
		}
		request.Namespace.Config = &persistencespb.NamespaceConfig{
			Retention:                    task.Config.GetWorkflowExecutionRetentionTtl(),
			HistoryArchivalState:         task.Config.GetHistoryArchivalState(),
			HistoryArchivalUri:           task.Config.GetHistoryArchivalUri(),
			VisibilityArchivalState:      task.Config.GetVisibilityArchivalState(),
			VisibilityArchivalUri:        task.Config.GetVisibilityArchivalUri(),
			CustomSearchAttributeAliases: task.Config.GetCustomSearchAttributeAliases(),
		}
		if task.Config.GetBadBinaries() != nil {
			request.Namespace.Config.BadBinaries = task.Config.GetBadBinaries()
		}
		request.Namespace.ReplicationConfig.Clusters = ConvertClusterReplicationConfigFromProto(task.ReplicationConfig.Clusters)
		request.Namespace.ConfigVersion = task.GetConfigVersion()
	}
	if resp.Namespace.FailoverVersion < task.GetFailoverVersion() {
		recordUpdated = true
		request.Namespace.ReplicationConfig.ActiveClusterName = task.ReplicationConfig.GetActiveClusterName()
		request.Namespace.ReplicationConfig.State = task.ReplicationConfig.GetState()
		request.Namespace.FailoverVersion = task.GetFailoverVersion()
		request.Namespace.FailoverNotificationVersion = notificationVersion
		request.Namespace.ReplicationConfig.FailoverHistory = ConvertFailoverHistoryToPersistenceProto(task.GetFailoverHistory())
	}

	if !recordUpdated {
		return OutcomeNoOpStale, nil
	}

	if err := h.metadataManager.UpdateNamespace(ctx, request); err != nil {
		return OutcomeUnspecified, err
	}
	return OutcomeApplied, nil
}

func (h *taskExecutorImpl) validateNamespaceReplicationTask(task *replicationspb.NamespaceTaskAttributes) error {
	if task == nil {
		return ErrEmptyNamespaceReplicationTask
	}

	if task.Id == "" {
		return ErrInvalidNamespaceID
	} else if task.Info == nil {
		return ErrInvalidNamespaceInfo
	} else if task.Config == nil {
		return ErrInvalidNamespaceConfig
	} else if task.ReplicationConfig == nil {
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
		res = append(res, &persistencespb.FailoverStatus{
			FailoverTime:    status.GetFailoverTime(),
			FailoverVersion: status.GetFailoverVersion(),
		})
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
