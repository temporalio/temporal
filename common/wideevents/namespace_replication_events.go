package wideevents

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const NamespaceReplicationLifecycleEventName = "namespace_replication_lifecycle"

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

// NamespaceReplicationLifecyclePayload records the transport and processing lifecycle of one
// namespace replication task. Task is the task attributes as they appeared on the queue; no
// namespace state is fetched to enrich the event.
type NamespaceReplicationLifecyclePayload struct {
	Phase              NamespaceReplicationPhase
	Outcome            NamespaceReplicationOutcome
	TaskType           int32
	TaskKind           string
	Namespace          string
	NamespaceID        string
	Operation          string
	ConfigVersion      *int64
	FailoverVersion    *int64
	SourceCluster      string
	TargetCluster      string
	SourceTaskID       *int64
	TaskFingerprint    string
	AttemptCount       int
	Error              string
	Task               string
	PersistenceRequest string
	Details            map[string]any
}

func (p NamespaceReplicationLifecyclePayload) EventName() string {
	return NamespaceReplicationLifecycleEventName
}

func (p NamespaceReplicationLifecyclePayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("phase", string(p.Phase)),
		log.Int64("task_type", int64(p.TaskType)),
		log.String("task_kind", p.TaskKind),
		log.String("namespace", p.Namespace),
		log.String("namespace_id", p.NamespaceID),
		log.String("operation", p.Operation),
		log.String("task_fingerprint", p.TaskFingerprint),
		log.String("task", p.Task),
	}
	if p.Outcome != "" {
		attrs = append(attrs, log.String("outcome", string(p.Outcome)))
	}
	if p.ConfigVersion != nil {
		attrs = append(attrs, log.Int64("config_version", *p.ConfigVersion))
	}
	if p.FailoverVersion != nil {
		attrs = append(attrs, log.Int64("failover_version", *p.FailoverVersion))
	}
	if p.SourceCluster != "" {
		attrs = append(attrs, log.String("source_cluster", p.SourceCluster))
	}
	if p.TargetCluster != "" {
		attrs = append(attrs, log.String("target_cluster", p.TargetCluster))
	}
	if p.SourceTaskID != nil {
		attrs = append(attrs, log.Int64("source_task_id", *p.SourceTaskID))
	}
	if p.AttemptCount > 0 {
		attrs = append(attrs, log.Int64("attempt_count", int64(p.AttemptCount)))
	}
	if p.Error != "" {
		attrs = append(attrs, log.String("error", p.Error))
	}
	if p.PersistenceRequest != "" {
		attrs = append(attrs, log.String("persistence_request", p.PersistenceRequest))
	}
	if len(p.Details) > 0 {
		attrs = append(attrs, jsonAttr("details", p.Details))
	}
	return attrs
}

type NamespaceReplicationLifecycleInput struct {
	Phase                  NamespaceReplicationPhase
	Outcome                NamespaceReplicationOutcome
	EventData              NamespaceReplicationTaskEventData
	SourceCluster          string
	TargetCluster          string
	SourceTaskID           *int64
	AttemptCount           int
	Error                  error
	CreateNamespaceRequest *persistence.CreateNamespaceRequest
	UpdateNamespaceRequest *persistence.UpdateNamespaceRequest
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

	payload := NamespaceReplicationLifecyclePayload{
		Phase:           in.Phase,
		Outcome:         in.Outcome,
		TaskType:        in.EventData.TaskType,
		TaskKind:        in.EventData.TaskKind,
		Namespace:       in.EventData.Namespace,
		NamespaceID:     in.EventData.NamespaceID,
		Operation:       in.EventData.Operation,
		ConfigVersion:   in.EventData.ConfigVersion,
		FailoverVersion: in.EventData.FailoverVersion,
		SourceCluster:   in.SourceCluster,
		TargetCluster:   in.TargetCluster,
		SourceTaskID:    in.SourceTaskID,
		TaskFingerprint: hex.EncodeToString(fingerprint[:]),
		AttemptCount:    in.AttemptCount,
		Task:            string(taskJSON),
		Details:         in.EventData.Details,
		PersistenceRequest: marshalNamespacePersistenceRequest(
			in.CreateNamespaceRequest,
			in.UpdateNamespaceRequest,
		),
	}
	if in.Error != nil {
		payload.Error = in.Error.Error()
	}
	Emit(logger, payload)
}

func marshalNamespacePersistenceRequest(
	createRequest *persistence.CreateNamespaceRequest,
	updateRequest *persistence.UpdateNamespaceRequest,
) string {
	type persistenceRequest struct {
		RequestType         string          `json:"request_type"`
		Namespace           json.RawMessage `json:"namespace"`
		IsGlobalNamespace   bool            `json:"is_global_namespace"`
		NotificationVersion *int64          `json:"notification_version,omitempty"`
	}

	var request persistenceRequest
	marshalNamespace := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}
	switch {
	case createRequest != nil:
		namespaceJSON, err := marshalNamespace.Marshal(createRequest.Namespace)
		if err != nil {
			return ""
		}
		request = persistenceRequest{
			RequestType:       "CreateNamespaceRequest",
			Namespace:         namespaceJSON,
			IsGlobalNamespace: createRequest.IsGlobalNamespace,
		}
	case updateRequest != nil:
		namespaceJSON, err := marshalNamespace.Marshal(updateRequest.Namespace)
		if err != nil {
			return ""
		}
		request = persistenceRequest{
			RequestType:         "UpdateNamespaceRequest",
			Namespace:           namespaceJSON,
			IsGlobalNamespace:   updateRequest.IsGlobalNamespace,
			NotificationVersion: &updateRequest.NotificationVersion,
		}
	default:
		return ""
	}

	requestJSON, err := json.Marshal(request)
	if err != nil {
		return ""
	}
	return string(requestJSON)
}
