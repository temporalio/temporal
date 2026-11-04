package namespacereplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNamespaceRepairObserverEmitsManualAndTerminalObservability(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)
	eventLogger := &nsreplEventCaptureLogger{}
	observer := &namespaceRepairObserver{
		eventLogger:                  eventLogger,
		metricsHandler:               metricsHandler,
		currentCluster:               "cluster-a",
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
	}
	request := &namespacereplicationpb.ReconcileNamespaceMutationRequest{
		RepairId: "repair-id",
		Reason:   "operator reason",
		Cutoff:   timestamppb.New(time.Unix(100, 0)),
	}
	terminal := &componentTerminalObservation{
		status:            namespacereplicationpb.COMPONENT_STATUS_COMPLETED,
		localOutcome:      namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
		peerOutcomes:      map[string]string{"cluster-b": namespacereplicationpb.PEER_APPLY_OUTCOME_MANUALLY_RECONCILED.String()},
		peerAttemptCounts: map[string]int{"cluster-b": 7},
		peerOutcomeCounts: map[string]int{namespacereplicationpb.PEER_APPLY_OUTCOME_MANUALLY_RECONCILED.String(): 1},
		peerCount:         1,
		peerAttemptCount:  7,
	}

	observer.observe(
		chasm.ComponentRef{BusinessID: "business-id", RunID: "run-id"},
		&replicationspb.NamespaceTaskAttributes{NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE},
		request,
		[]manualPeerReconciliationObservation{{
			targetCell:   "cluster-b",
			basis:        enumsspb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET,
			attemptCount: 7,
			lastFailure:  "peer unavailable",
		}},
		terminal,
	)

	recordings := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMManualReconciliations.Name())
	require.Len(t, recordings, 1)
	require.Equal(t, "cluster-a", recordings[0].Tags[metrics.SourceClusterTag("").Key])
	require.Equal(t, "cluster-b", recordings[0].Tags[metrics.TargetClusterTag("").Key])
	require.Equal(t, enumsspb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET.String(), recordings[0].Tags[manualReconciliationBasisTag])
	_, repairIDIsMetricLabel := recordings[0].Tags["repair_id"]
	require.False(t, repairIDIsMetricLabel)
	peerEvents := authoritativeEventMatches(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStagePeer,
		nsreplicationcommon.CHASMApplyOutcomeManuallyReconciled,
	)
	require.Len(t, peerEvents, 1)
	require.Equal(t, nsreplicationcommon.OperatorRepair, peerEvents[0]["transport"])
	require.Equal(t, "repair-id", peerEvents[0]["repair_id"])
	require.Equal(t, "peer unavailable", peerEvents[0]["last_delivery_failure"])
	componentEvents := authoritativeEventMatches(
		t,
		eventLogger,
		nsreplicationcommon.CHASMApplyStageComponent,
		nsreplicationcommon.CHASMApplyOutcomeCompleted,
	)
	require.Len(t, componentEvents, 1)
	require.Equal(t, nsreplicationcommon.OperatorRepair, componentEvents[0]["transport"])
	require.Equal(t, nsreplicationcommon.OperatorRepair, componentEvents[0]["mode"])
	require.Equal(t, "repair-id", componentEvents[0]["repair_id"])
}
