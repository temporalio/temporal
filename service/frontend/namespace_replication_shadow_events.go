package frontend

import (
	"encoding/hex"
	"encoding/json"
	"maps"

	otellog "go.opentelemetry.io/otel/log"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/wideevents"
	"google.golang.org/protobuf/encoding/protojson"
)

const (
	namespaceReplicationShadowTransport = "chasm"
	namespaceReplicationShadowMode      = "shadow"

	namespaceReplicationComparisonBoundaryBuild   = "build"
	namespaceReplicationComparisonBoundaryReceive = "receive"
)

func emitNamespaceReplicationShadowComparison(
	eventLogger otellog.Logger,
	task *replicationspb.NamespaceTaskAttributes,
	boundary string,
	outcome string,
	sourceCluster string,
	targetCluster string,
	attemptCount int,
	details map[string]any,
	comparisonErr error,
) {
	eventData, ok := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(
		&replicationspb.ReplicationTask{
			TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
			Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
				NamespaceTaskAttributes: task,
			},
		},
	)
	if !ok {
		return
	}
	eventData.Details = maps.Clone(details)
	if eventData.Details == nil {
		eventData.Details = make(map[string]any)
	}
	eventData.Details["transport"] = namespaceReplicationShadowTransport
	eventData.Details["mode"] = namespaceReplicationShadowMode
	eventData.Details["comparison_boundary"] = boundary

	wideevents.EmitNamespaceReplicationLifecycle(eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase:                        wideevents.NamespaceReplicationCompared,
		Outcome:                      wideevents.NamespaceReplicationOutcome(outcome),
		EventData:                    eventData,
		SourceCluster:                sourceCluster,
		TargetCluster:                targetCluster,
		AttemptCount:                 attemptCount,
		Error:                        comparisonErr,
		EmitOnTaskSerializationError: true,
	})
}

func (d *namespaceHandler) emitShadowBuildComparison(
	legacyTask *replicationspb.NamespaceTaskAttributes,
	chasmTask *replicationspb.NamespaceTaskAttributes,
	legacyFingerprint []byte,
	chasmFingerprint []byte,
	differingFields []string,
	componentBusinessID string,
	sourceCluster string,
	targetClusters []string,
	outcome string,
	comparisonErr error,
) {
	if d.config == nil || d.config.EmitNamespaceLifecycleEvents == nil || !d.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := make(map[string]any, 6)
	if componentBusinessID != "" {
		details["component_business_id"] = componentBusinessID
	}
	details["target_clusters"] = targetClusters
	if len(legacyFingerprint) > 0 {
		details["legacy_task_fingerprint"] = hex.EncodeToString(legacyFingerprint)
	}
	if len(chasmFingerprint) > 0 {
		details["chasm_task_fingerprint"] = hex.EncodeToString(chasmFingerprint)
	}
	if len(differingFields) > 0 {
		details["differing_fields"] = differingFields
	}
	if len(differingFields) > 0 || comparisonErr != nil && len(legacyFingerprint) == 0 {
		legacyTaskJSON, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(legacyTask)
		if err == nil {
			details["legacy_task"] = json.RawMessage(legacyTaskJSON)
		} else {
			details["legacy_task_payload_status"] = "incomplete"
			details["legacy_task_json_error"] = err.Error()
		}
	}
	emitNamespaceReplicationShadowComparison(
		d.eventLogger,
		chasmTask,
		namespaceReplicationComparisonBoundaryBuild,
		outcome,
		sourceCluster,
		"",
		0,
		details,
		comparisonErr,
	)
}

func (adh *AdminHandler) emitShadowReceiveComparison(
	request *adminservice.ApplyNamespaceMutationRequest,
	expectedFingerprint []byte,
	actualFingerprint []byte,
	outcome string,
	comparisonErr error,
) {
	if adh.config == nil || adh.config.EmitNamespaceLifecycleEvents == nil || !adh.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := make(map[string]any, 4)
	if request.GetComponentBusinessId() != "" {
		details["component_business_id"] = request.GetComponentBusinessId()
	}
	if request.GetComponentRunId() != "" {
		details["component_run_id"] = request.GetComponentRunId()
	}
	if len(expectedFingerprint) > 0 {
		details["expected_task_fingerprint"] = hex.EncodeToString(expectedFingerprint)
	}
	if len(actualFingerprint) > 0 {
		details["actual_task_fingerprint"] = hex.EncodeToString(actualFingerprint)
	}
	emitNamespaceReplicationShadowComparison(
		adh.eventLogger,
		request.GetNamespaceTask(),
		namespaceReplicationComparisonBoundaryReceive,
		outcome,
		request.GetSourceCluster(),
		adh.clusterMetadata.GetCurrentClusterName(),
		int(request.GetAttemptCount()),
		details,
		comparisonErr,
	)
}
