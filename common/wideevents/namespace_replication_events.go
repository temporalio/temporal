package wideevents

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"

	"go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// NamespaceReplicationLifecycleEventName aliases NamespaceLifecycleEventName for compatibility.
// TODO: Remove it after callers migrate to NamespaceLifecycleEventName.
const NamespaceReplicationLifecycleEventName = NamespaceLifecycleEventName

type NamespaceReplicationPhase string

type NamespaceReplicationOutcome string

const (
	NamespaceReplicationCreated   NamespaceReplicationPhase = "created"
	NamespaceReplicationReceived  NamespaceReplicationPhase = "received"
	NamespaceReplicationProcessed NamespaceReplicationPhase = "processed"
	NamespaceReplicationDLQed     NamespaceReplicationPhase = "dlqed"

	NamespaceReplicationOutcomeCreated     NamespaceReplicationOutcome = "created"
	NamespaceReplicationOutcomeUpdated     NamespaceReplicationOutcome = "updated"
	NamespaceReplicationOutcomeNotAdmitted NamespaceReplicationOutcome = "not_admitted"
	NamespaceReplicationOutcomeDuplicate   NamespaceReplicationOutcome = "duplicate"
	NamespaceReplicationOutcomeNoChange    NamespaceReplicationOutcome = "no_change"
)

// NamespaceReplicationTaskEventData contains the task-specific fields shared by every lifecycle phase.
// TaskFingerprintData may be set when the bytes carried by the queue are more stable than
// deterministically marshaling TaskPayload.
type NamespaceReplicationTaskEventData struct {
	TaskType            int32
	TaskKind            string
	Namespace           string
	NamespaceID         string
	Operation           string
	ConfigVersion       *int64
	FailoverVersion     *int64
	TaskPayload         proto.Message
	TaskFingerprintData []byte
	Details             map[string]any
}

// NamespaceReplicationTaskEventDataProvider identifies namespace replication tasks and extracts the
// task-specific data needed by the lifecycle event.
type NamespaceReplicationTaskEventDataProvider interface {
	Extract(task *replicationspb.ReplicationTask) (NamespaceReplicationTaskEventData, bool)
}

type defaultNamespaceReplicationTaskEventDataProvider struct{}

// NewDefaultNamespaceReplicationTaskEventDataProvider returns the OSS namespace task provider.
func NewDefaultNamespaceReplicationTaskEventDataProvider() NamespaceReplicationTaskEventDataProvider {
	return defaultNamespaceReplicationTaskEventDataProvider{}
}

func (defaultNamespaceReplicationTaskEventDataProvider) Extract(
	task *replicationspb.ReplicationTask,
) (NamespaceReplicationTaskEventData, bool) {
	if task.GetTaskType() != enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK {
		return NamespaceReplicationTaskEventData{}, false
	}

	attributes := task.GetNamespaceTaskAttributes()
	if attributes == nil {
		return NamespaceReplicationTaskEventData{}, false
	}

	configVersion := attributes.GetConfigVersion()
	failoverVersion := attributes.GetFailoverVersion()
	return NamespaceReplicationTaskEventData{
		TaskType:        int32(task.GetTaskType()),
		TaskKind:        "namespace",
		Namespace:       attributes.GetInfo().GetName(),
		NamespaceID:     attributes.GetId(),
		Operation:       attributes.GetNamespaceOperation().String(),
		ConfigVersion:   &configVersion,
		FailoverVersion: &failoverVersion,
		TaskPayload:     attributes,
	}, true
}

// NamespaceReplicationLifecyclePayload uses the namespace lifecycle envelope. Replication-specific
// data is carried in Details.
type NamespaceReplicationLifecyclePayload NamespaceLifecyclePayload

func (p NamespaceReplicationLifecyclePayload) EventName() string {
	return NamespaceLifecycleEventName
}

func (p NamespaceReplicationLifecyclePayload) Attributes() []log.KeyValue {
	return NamespaceLifecyclePayload(p).Attributes()
}

type NamespaceReplicationLifecycleInput struct {
	Phase                     NamespaceReplicationPhase
	Outcome                   NamespaceReplicationOutcome
	EventData                 NamespaceReplicationTaskEventData
	SourceCluster             string
	TargetCluster             string
	SourceTaskID              *int64
	AttemptCount              int
	Error                     error
	LocalNamespacePreMutation *persistencespb.NamespaceDetail
	CreateNamespaceRequest    *persistence.CreateNamespaceRequest
	UpdateNamespaceRequest    *persistence.UpdateNamespaceRequest
}

func EmitNamespaceReplicationLifecycle(logger log.Logger, in NamespaceReplicationLifecycleInput) {
	if in.EventData.TaskPayload == nil {
		return
	}

	taskJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(in.EventData.TaskPayload)
	if err != nil {
		return
	}
	taskBytes := in.EventData.TaskFingerprintData
	if len(taskBytes) == 0 {
		taskBytes, err = proto.MarshalOptions{Deterministic: true}.Marshal(in.EventData.TaskPayload)
		if err != nil {
			return
		}
	}
	fingerprint := sha256.Sum256(taskBytes)

	details := make(map[string]any, len(in.EventData.Details)+12)
	maps.Copy(details, in.EventData.Details)
	details["task_type"] = in.EventData.TaskType
	details["task_kind"] = in.EventData.TaskKind
	details["operation"] = in.EventData.Operation
	details["task_fingerprint"] = hex.EncodeToString(fingerprint[:])
	details["task"] = json.RawMessage(taskJSON)
	if in.Outcome != "" {
		details["outcome"] = in.Outcome
	}
	if in.EventData.ConfigVersion != nil {
		details["config_version"] = *in.EventData.ConfigVersion
	}
	if in.EventData.FailoverVersion != nil {
		details["failover_version"] = *in.EventData.FailoverVersion
	}
	if in.SourceCluster != "" {
		details["source_cluster"] = in.SourceCluster
	}
	if in.TargetCluster != "" {
		details["target_cluster"] = in.TargetCluster
	}
	if in.SourceTaskID != nil {
		details["source_task_id"] = *in.SourceTaskID
	}
	if in.AttemptCount > 0 {
		details["attempt_count"] = in.AttemptCount
	}
	if in.Error != nil {
		details["error"] = in.Error.Error()
	}
	if localNamespacePreMutation := marshalNamespaceDetail(in.LocalNamespacePreMutation); len(localNamespacePreMutation) > 0 {
		details["local_namespace_pre_mutation"] = localNamespacePreMutation
	}
	if persistenceRequest := marshalNamespacePersistenceRequest(
		in.CreateNamespaceRequest,
		in.UpdateNamespaceRequest,
	); len(persistenceRequest) > 0 {
		details["persistence_request"] = persistenceRequest
	}
	Emit(logger, NamespaceReplicationLifecyclePayload{
		Phase:       string(in.Phase),
		Namespace:   in.EventData.Namespace,
		NamespaceID: in.EventData.NamespaceID,
		Details:     details,
	})
}

func marshalNamespaceDetail(namespace *persistencespb.NamespaceDetail) json.RawMessage {
	if namespace == nil {
		return nil
	}
	namespaceJSON, err := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}.Marshal(namespace)
	if err != nil {
		return nil
	}
	return namespaceJSON
}

func marshalNamespacePersistenceRequest(
	createRequest *persistence.CreateNamespaceRequest,
	updateRequest *persistence.UpdateNamespaceRequest,
) json.RawMessage {
	type persistenceRequest struct {
		RequestType         string          `json:"request_type"`
		Namespace           json.RawMessage `json:"namespace"`
		IsGlobalNamespace   bool            `json:"is_global_namespace"`
		NotificationVersion *int64          `json:"notification_version,omitempty"`
	}

	var request persistenceRequest
	switch {
	case createRequest != nil:
		namespaceJSON := marshalNamespaceDetail(createRequest.Namespace)
		if len(namespaceJSON) == 0 {
			return nil
		}
		request = persistenceRequest{
			RequestType:       "CreateNamespaceRequest",
			Namespace:         json.RawMessage(namespaceJSON),
			IsGlobalNamespace: createRequest.IsGlobalNamespace,
		}
	case updateRequest != nil:
		namespaceJSON := marshalNamespaceDetail(updateRequest.Namespace)
		if len(namespaceJSON) == 0 {
			return nil
		}
		request = persistenceRequest{
			RequestType:         "UpdateNamespaceRequest",
			Namespace:           json.RawMessage(namespaceJSON),
			IsGlobalNamespace:   updateRequest.IsGlobalNamespace,
			NotificationVersion: &updateRequest.NotificationVersion,
		}
	default:
		return nil
	}

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return nil
	}
	return requestJSON
}
