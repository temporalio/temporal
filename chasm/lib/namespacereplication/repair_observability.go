package namespacereplication

import (
	"maps"
	"time"

	otellog "go.opentelemetry.io/otel/log"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
	"go.uber.org/fx"
)

const manualReconciliationBasisTag = "reconciliation_basis"

type namespaceRepairObserverOptions struct {
	fx.In

	Logger          log.Logger
	EventLogger     otellog.Logger
	MetricsHandler  metrics.Handler
	ClusterMetadata cluster.Metadata
	DynamicConfig   *dynamicconfig.Collection
}

type namespaceRepairObserver struct {
	logger                       log.Logger
	eventLogger                  otellog.Logger
	metricsHandler               metrics.Handler
	currentCluster               string
	emitNamespaceLifecycleEvents dynamicconfig.BoolPropertyFn
}

type manualPeerReconciliationObservation struct {
	targetCell   string
	basis        enumsspb.ManualReconciliationBasis
	attemptCount int32
	lastFailure  string
}

func newNamespaceRepairObserver(options namespaceRepairObserverOptions) *namespaceRepairObserver {
	return &namespaceRepairObserver{
		logger:                       options.Logger,
		eventLogger:                  options.EventLogger,
		metricsHandler:               options.MetricsHandler,
		currentCluster:               options.ClusterMetadata.GetCurrentClusterName(),
		emitNamespaceLifecycleEvents: dynamicconfig.EmitNamespaceLifecycleEvents.Get(options.DynamicConfig),
	}
}

func (o *namespaceRepairObserver) observe(
	ref chasm.ComponentRef,
	task *replicationspb.NamespaceTaskAttributes,
	request *namespacereplicationpb.ReconcileNamespaceMutationRequest,
	peers []manualPeerReconciliationObservation,
	terminal *componentTerminalObservation,
) {
	var telemetryPanic error
	defer log.CapturePanic(o.logger, &telemetryPanic)
	emitEvents := o.emitNamespaceLifecycleEvents != nil && o.emitNamespaceLifecycleEvents()
	var eventDetails map[string]any
	if emitEvents {
		eventDetails = repairEventDetails(request)
	}
	if terminal != nil && terminal.shadowCompletion != nil {
		observer := shadowCompletionObserver{logger: o.logger, metricsHandler: o.metricsHandler, eventLogger: o.eventLogger,
			emitNamespaceLifecycleEvents: func() bool { return emitEvents }, sourceCluster: o.currentCluster}
		observer.observe(ref, terminal.shadowCompletion)
	}
	for _, peer := range peers {
		metrics.NamespaceReplicationCHASMManualReconciliations.With(o.metricsHandler).Record(
			1,
			metrics.SourceClusterTag(o.currentCluster),
			metrics.TargetClusterTag(peer.targetCell),
			metrics.OperationTag(nsreplicationcommon.OperationMetricValue(task.GetNamespaceOperation())),
			metrics.OutcomeTag(nsreplicationcommon.CHASMApplyOutcomeManuallyReconciled),
			metrics.StringTag(manualReconciliationBasisTag, peer.basis.String()),
		)
		if !emitEvents {
			continue
		}
		details := maps.Clone(eventDetails)
		details["reconciliation_basis"] = peer.basis.String()
		if peer.lastFailure != "" {
			details["last_delivery_failure"] = peer.lastFailure
		}
		nsreplicationcommon.EmitCHASMAuthoritativeApply(
			o.eventLogger,
			nsreplicationcommon.CHASMAuthoritativeApplyObservation{
				Task:                task,
				Stage:               nsreplicationcommon.CHASMApplyStagePeer,
				Outcome:             nsreplicationcommon.CHASMApplyOutcomeManuallyReconciled,
				SourceCluster:       o.currentCluster,
				TargetCluster:       peer.targetCell,
				ComponentBusinessID: ref.BusinessID,
				ComponentRunID:      ref.RunID,
				AttemptCount:        int(peer.attemptCount),
				Details:             details,
				Transport:           nsreplicationcommon.OperatorRepair,
				Mode:                nsreplicationcommon.OperatorRepair,
			},
		)
	}
	if terminal != nil && emitEvents {
		observation := componentTerminalApplyObservation(o.currentCluster, ref, task, terminal, nil)
		observation.Transport = nsreplicationcommon.OperatorRepair
		observation.Mode = nsreplicationcommon.OperatorRepair
		maps.Copy(observation.Details, eventDetails)
		nsreplicationcommon.EmitCHASMAuthoritativeApply(o.eventLogger, observation)
	}
}

func repairEventDetails(request *namespacereplicationpb.ReconcileNamespaceMutationRequest) map[string]any {
	return map[string]any{
		"verified_config_version":   request.GetVerifiedConfigVersion().GetValue(),
		"verified_failover_version": request.GetVerifiedFailoverVersion().GetValue(),
		"repair_id":                 request.GetRepairId(),
		"operator_reason":           request.GetReason(),
		"reconciliation_cutoff":     request.GetCutoff().AsTime().Format(time.RFC3339Nano),
	}
}
