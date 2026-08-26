package metrics

import (
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally/v4"
	tallyprometheus "github.com/uber-go/tally/v4/prometheus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/tqid"
)

func TestActivityOperatorMetricsUseCompatiblePrometheusDescriptors(t *testing.T) {
	registry := prometheus.NewRegistry()
	var reporterErrorsMu sync.Mutex
	var reporterErrors []error
	reporter := tallyprometheus.NewReporter(tallyprometheus.Options{
		Registerer: registry,
		OnRegisterError: func(err error) {
			reporterErrorsMu.Lock()
			defer reporterErrorsMu.Unlock()
			reporterErrors = append(reporterErrors, err)
		},
	})
	scope, closer := tally.NewRootScope(tally.ScopeOptions{
		CachedReporter:         reporter,
		OmitCardinalityMetrics: true,
	}, time.Hour)
	t.Cleanup(func() {
		require.NoError(t, closer.Close())
	})

	handler := NewTallyMetricsHandler(ClientConfig{}, scope)
	activityMetrics := []struct {
		metric    counterDefinition
		operation string
	}{
		{ActivityUpdateOptions, ActivityUpdateOptionsScope},
		{ActivityPause, ActivityPausedScope},
		{ActivityUnpause, ActivityUnpausedScope},
		{ActivityReset, ActivityResetScope},
	}
	for _, activityMetric := range activityMetrics {
		wfaHandler := GetPerActivityScope(
			handler,
			"namespace",
			tqid.UnsafeTaskQueueFamily("namespace", "wfa-task-queue"),
			true,
			activityMetric.operation,
			"wfa-activity-type",
			"workflow-type",
			enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
		)
		saaHandler := GetPerActivityScope(
			handler,
			"namespace",
			tqid.UnsafeTaskQueueFamily("namespace", "saa-task-queue"),
			true,
			activityMetric.operation,
			"saa-activity-type",
			"standalone-activity",
			enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED,
		)
		activityMetric.metric.With(wfaHandler).Record(1)
		activityMetric.metric.With(saaHandler).Record(1)
	}

	reporterErrorsMu.Lock()
	defer reporterErrorsMu.Unlock()
	require.Empty(t, reporterErrors)

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, len(activityMetrics))
}
