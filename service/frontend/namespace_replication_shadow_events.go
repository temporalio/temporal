package frontend

import (
	"encoding/hex"
	"maps"

	otellog "go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/wideevents"
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
		Phase:         wideevents.NamespaceReplicationCompared,
		Outcome:       wideevents.NamespaceReplicationOutcome(outcome),
		EventData:     eventData,
		SourceCluster: sourceCluster,
		TargetCluster: targetCluster,
		Error:         comparisonErr,
	})
}

func (d *namespaceHandler) emitShadowBuildComparison(
	task *replicationspb.NamespaceTaskAttributes,
	legacyFingerprint []byte,
	chasmFingerprint []byte,
	differingFields []string,
	outcome string,
	comparisonErr error,
) {
	if d.config == nil || d.config.EmitNamespaceLifecycleEvents == nil || !d.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := make(map[string]any, 3)
	if len(legacyFingerprint) > 0 {
		details["legacy_task_fingerprint"] = hex.EncodeToString(legacyFingerprint)
	}
	if len(chasmFingerprint) > 0 {
		details["chasm_task_fingerprint"] = hex.EncodeToString(chasmFingerprint)
	}
	if len(differingFields) > 0 {
		details["differing_fields"] = differingFields
	}
	emitNamespaceReplicationShadowComparison(
		d.eventLogger,
		task,
		namespaceReplicationComparisonBoundaryBuild,
		outcome,
		d.clusterMetadata.GetCurrentClusterName(),
		"",
		details,
		comparisonErr,
	)
}

func (adh *AdminHandler) emitShadowReceiveComparison(
	task *replicationspb.NamespaceTaskAttributes,
	expectedFingerprint []byte,
	actualFingerprint []byte,
	outcome string,
	comparisonErr error,
) {
	if adh.config == nil || adh.config.EmitNamespaceLifecycleEvents == nil || !adh.config.EmitNamespaceLifecycleEvents() {
		return
	}
	details := make(map[string]any, 2)
	if len(expectedFingerprint) > 0 {
		details["expected_task_fingerprint"] = hex.EncodeToString(expectedFingerprint)
	}
	if len(actualFingerprint) > 0 {
		details["actual_task_fingerprint"] = hex.EncodeToString(actualFingerprint)
	}
	emitNamespaceReplicationShadowComparison(
		adh.eventLogger,
		task,
		namespaceReplicationComparisonBoundaryReceive,
		outcome,
		"",
		adh.clusterMetadata.GetCurrentClusterName(),
		details,
		comparisonErr,
	)
}
