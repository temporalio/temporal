package frontend

import (
	"encoding/hex"
	"encoding/json"

	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func shadowReplicationEventData(task *replicationspb.NamespaceTaskAttributes, payload []byte, details map[string]any) wideevents.NamespaceReplicationTaskEventData {
	missingTask := task == nil
	if task == nil {
		task = &replicationspb.NamespaceTaskAttributes{}
		details["task_payload_status"] = "unavailable"
	}
	data, _ := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(&replicationspb.ReplicationTask{
		TaskType:   enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{NamespaceTaskAttributes: task},
	})
	data.TaskFingerprintData = payload
	if missingTask {
		data.TaskPayload = nil
		data.ConfigVersion = nil
		data.FailoverVersion = nil
	}
	data.Details = details
	data.Details["transport"] = "chasm"
	data.Details["mode"] = "shadow"
	return data
}

func (d *namespaceHandler) recordShadowSourceOutcome(task *replicationspb.NamespaceTaskAttributes, payload []byte, businessID string, peers []string, stage string, observationErr error) {
	var telemetryPanic error
	defer log.CapturePanic(d.logger, &telemetryPanic)

	outcome := "prepared"
	metric := metrics.NamespaceReplicationShadowPreparationOutcomes
	if stage == "trigger" {
		metric = metrics.NamespaceReplicationShadowTriggerOutcomes
		outcome = "rpc_completed"
	}
	if observationErr != nil {
		outcome = "error"
		if stage == "trigger" {
			outcome = "rpc_error"
		}
	}
	source := d.clusterMetadata.GetCurrentClusterName()
	if d.metricsHandler != nil {
		metric.With(d.metricsHandler).Record(1, metrics.SourceClusterTag(source),
			metrics.OperationTag(nsreplication.OperationMetricValue(task.GetNamespaceOperation())), metrics.OutcomeTag(outcome))
	}
	if stage == "trigger" || d.config == nil || d.config.EmitNamespaceLifecycleEvents == nil || !d.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := map[string]any{"component_business_id": businessID, "target_clusters": peers, "apply_stage": stage}
	wideevents.EmitNamespaceReplicationLifecycle(d.eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase: wideevents.NamespaceReplicationPhase(stage), Outcome: wideevents.NamespaceReplicationOutcome(outcome),
		EventData: shadowReplicationEventData(task, payload, details), SourceCluster: source,
		Error: observationErr, EmitOnTaskSerializationError: true,
	})
}

func (adh *AdminHandler) recordShadowReceiveComparison(request *adminservice.ApplyNamespaceMutationRequest, task, legacyTask *replicationspb.NamespaceTaskAttributes, outcome, reason string, comparisonErr error) {
	// Telemetry failures must not change the RPC result.
	var telemetryPanic error
	defer log.CapturePanic(adh.logger, &telemetryPanic)

	// Validation may reject the request before decoding its payload. Decode only
	// for diagnostics here; the recorded validation outcome remains unchanged.
	if task == nil && len(request.GetNamespaceTaskPayload()) > 0 {
		task = &replicationspb.NamespaceTaskAttributes{}
		if err := proto.Unmarshal(request.GetNamespaceTaskPayload(), task); err != nil {
			task = nil
		}
	}
	if legacyTask == nil && len(request.GetShadowLegacyTaskPayload()) > 0 {
		legacyTask = &replicationspb.NamespaceTaskAttributes{}
		if err := proto.Unmarshal(request.GetShadowLegacyTaskPayload(), legacyTask); err != nil {
			legacyTask = nil
		}
	}
	// Attribute the comparison to the intended mutation, even when CHASM sent different metadata.
	metadataTask := legacyTask
	if metadataTask == nil {
		metadataTask = task
	}
	if adh.metricsHandler != nil {
		metrics.NamespaceReplicationShadowReceiveComparisonOutcomes.With(adh.metricsHandler).Record(1,
			metrics.SourceClusterTag(request.GetSourceCluster()), metrics.TargetClusterTag(adh.clusterMetadata.GetCurrentClusterName()),
			metrics.OperationTag(nsreplication.OperationMetricValue(metadataTask.GetNamespaceOperation())), metrics.OutcomeTag(outcome), metrics.StringTag("reason", reason))
	}
	if adh.config == nil || adh.config.EmitNamespaceLifecycleEvents == nil || !adh.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := map[string]any{
		"comparison_boundary": "receive", "reason": reason,
		"component_business_id": request.GetComponentBusinessId(), "component_run_id": request.GetComponentRunId(),
	}
	if len(request.GetShadowLegacyTaskPayload()) > 0 {
		details["legacy_task_fingerprint"] = hex.EncodeToString(nsreplication.NamespaceTaskFingerprintFromPayload(request.GetShadowLegacyTaskPayload()))
	}
	if outcome == "mismatch" && legacyTask != nil {
		if task != nil && reason == "legacy_payload_mismatch" {
			details["differing_fields"] = nsreplication.DifferingNamespaceTaskFields(legacyTask, task)
		}
		encoded, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(legacyTask)
		if err == nil {
			details["legacy_task"] = json.RawMessage(encoded)
		} else {
			details["legacy_task_payload_status"] = "incomplete"
			details["legacy_task_json_error"] = err.Error()
		}
	}
	data := shadowReplicationEventData(task, request.GetNamespaceTaskPayload(), details)
	if legacyTask != nil {
		data.NamespaceID = metadataTask.GetId()
		data.Namespace = metadataTask.GetInfo().GetName()
		data.Operation = metadataTask.GetNamespaceOperation().String()
		data.ConfigVersion = new(metadataTask.GetConfigVersion())
		data.FailoverVersion = new(metadataTask.GetFailoverVersion())
		data.Details["metadata_source"] = "legacy_reference"
	}
	wideevents.EmitNamespaceReplicationLifecycle(adh.eventLogger, wideevents.NamespaceReplicationLifecycleInput{
		Phase: wideevents.NamespaceReplicationCompared, Outcome: wideevents.NamespaceReplicationOutcome(outcome),
		EventData:     data,
		SourceCluster: request.GetSourceCluster(), TargetCluster: adh.clusterMetadata.GetCurrentClusterName(),
		AttemptCount: int(request.GetAttemptCount()), Error: comparisonErr, EmitOnTaskSerializationError: true,
	})
}
