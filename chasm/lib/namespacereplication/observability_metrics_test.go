package namespacereplication

import (
	"strings"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	tallyprom "github.com/uber-go/tally/v4/prometheus"
	enumsspb "go.temporal.io/server/api/enums/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
)

func TestCHASMApplyMetricsUseConsistentPrometheusLabels(t *testing.T) {
	t.Parallel()
	registry := prom.NewRegistry()
	var registrationErrors []error
	reporter := tallyprom.NewReporter(tallyprom.Options{Registerer: registry, OnRegisterError: func(err error) {
		registrationErrors = append(registrationErrors, err)
	}})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{CachedReporter: reporter}, time.Hour)
	handler := metrics.NewTallyMetricsHandler(metrics.ClientConfig{}, scope)
	detail := testDetail()
	nsreplication.RecordCHASMAuthoritativeApply(handler, nsreplication.CHASMAuthoritativeApplyObservation{
		Task:  nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail),
		Stage: "local", Outcome: "applied", SourceCluster: "cellA", TargetCluster: "cellA",
	})
	peer := &applyPeerTaskHandler{logger: log.NewNoopLogger(), metricsHandler: handler}
	peer.observePeerApply(PeerApplyRequest{Detail: detail, Operation: enumsspb.NAMESPACE_OPERATION_UPDATE, SourceCluster: "cellA", TargetCluster: "cellB"},
		peerOutcomeRecord{stateUpdated: true, outcome: namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED}, nil, nil, time.Second)
	require.NoError(t, closer.Close())
	require.Empty(t, registrationErrors)
	families, err := registry.Gather()
	require.NoError(t, err)
	found := false
	for _, family := range families {
		if strings.HasPrefix(family.GetName(), metrics.NamespaceReplicationCHASMApplyOutcomes.Name()) {
			found = true
			require.Len(t, family.Metric, 2, "both local and peer outcomes must remain exportable")
		}
	}
	require.True(t, found)
}
