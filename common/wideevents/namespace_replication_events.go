package wideevents

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"go.opentelemetry.io/otel/log"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const NamespaceReplicationLifecycleEventName = "namespace_replication_lifecycle"

type NamespaceReplicationPhase string

const (
	NamespaceReplicationCreated   NamespaceReplicationPhase = "created"
	NamespaceReplicationReceived  NamespaceReplicationPhase = "received"
	NamespaceReplicationProcessed NamespaceReplicationPhase = "processed"
	NamespaceReplicationDLQed     NamespaceReplicationPhase = "dlqed"
)

// NamespaceReplicationLifecyclePayload records the transport and processing lifecycle of one
// namespace replication task. Task is the task attributes as they appeared on the queue; no
// namespace state is fetched to enrich the event.
type NamespaceReplicationLifecyclePayload struct {
	Phase              NamespaceReplicationPhase
	Namespace          string
	NamespaceID        string
	Operation          string
	ConfigVersion      int64
	FailoverVersion    int64
	SourceCluster      string
	TargetCluster      string
	SourceTaskID       *int64
	TaskFingerprint    string
	AttemptCount       int
	Error              string
	Task               string
	PersistenceRequest string
}

func (p NamespaceReplicationLifecyclePayload) EventName() string {
	return NamespaceReplicationLifecycleEventName
}

func (p NamespaceReplicationLifecyclePayload) Attributes() []log.KeyValue {
	attrs := []log.KeyValue{
		log.String("phase", string(p.Phase)),
		log.String("namespace", p.Namespace),
		log.String("namespace_id", p.NamespaceID),
		log.String("operation", p.Operation),
		log.Int64("config_version", p.ConfigVersion),
		log.Int64("failover_version", p.FailoverVersion),
		log.String("task_fingerprint", p.TaskFingerprint),
		log.String("task", p.Task),
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
	return attrs
}

type NamespaceReplicationLifecycleInput struct {
	Phase                  NamespaceReplicationPhase
	Task                   *replicationspb.NamespaceTaskAttributes
	SourceCluster          string
	TargetCluster          string
	SourceTaskID           *int64
	AttemptCount           int
	Error                  error
	CreateNamespaceRequest *persistence.CreateNamespaceRequest
	UpdateNamespaceRequest *persistence.UpdateNamespaceRequest
}

func EmitNamespaceReplicationLifecycle(logger log.Logger, in NamespaceReplicationLifecycleInput) {
	if in.Task == nil {
		return
	}

	taskJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(in.Task)
	if err != nil {
		return
	}
	taskBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(in.Task)
	if err != nil {
		return
	}
	fingerprint := sha256.Sum256(taskBytes)

	payload := NamespaceReplicationLifecyclePayload{
		Phase:           in.Phase,
		Namespace:       in.Task.GetInfo().GetName(),
		NamespaceID:     in.Task.GetId(),
		Operation:       in.Task.GetNamespaceOperation().String(),
		ConfigVersion:   in.Task.GetConfigVersion(),
		FailoverVersion: in.Task.GetFailoverVersion(),
		SourceCluster:   in.SourceCluster,
		TargetCluster:   in.TargetCluster,
		SourceTaskID:    in.SourceTaskID,
		TaskFingerprint: hex.EncodeToString(fingerprint[:]),
		AttemptCount:    in.AttemptCount,
		Task:            string(taskJSON),
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
