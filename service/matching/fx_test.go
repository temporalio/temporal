package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/matching/configs"
)

const (
	apiAddActivity   = "/temporal.server.api.matchingservice.v1.MatchingService/AddActivityTask"
	apiCancelPolls   = "/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingWorkerPolls"
	apiUnknownMethod = "/temporal.server.api.matchingservice.v1.MatchingService/SomeUnknownMethod"
)

func makeTestPriorityFn(
	t *testing.T,
	hostRPS int,
	nsFairShare map[string]float64,
) (quotas.RequestPriorityFn, []int) {
	t.Helper()
	cfg := &Config{
		RPS: func() int { return hostRPS },
		NamespaceMatchingFairShare: func(ns string) float64 {
			return nsFairShare[ns]
		},
		OperatorRPSRatio: dynamicconfig.GetFloatPropertyFn(0.2),
	}
	return getFairnessPriorityFn(cfg, metrics.NoopMetricsHandler)
}

func req(api, caller, callerType string) quotas.Request {
	return quotas.NewRequest(api, 1, caller, callerType, 0, "")
}

func TestFairnessPriorityFn_MultipleCallers(t *testing.T) {
	// namespaceA: 50% share, namespaceB: 20% share, namespaceC: fairness off.
	nsFairShare := map[string]float64{
		"namespaceA": 0.5,
		"namespaceB": 0.2,
	}
	priorityFn, priorities := makeTestPriorityFn(t, 100, nsFairShare)
	require.Equal(t, []int{0, 1, 2, 3, 4}, priorities)

	// In-share callers keep their API priority.
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))

	// Drain namespaceA's bucket (share=50, burst=100).
	for range 150 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	// Over-share: every caller type except Preemptable collapses to band 3.
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeOperator)))
	require.Equal(t, 3, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiUnknownMethod, "namespaceA", headers.CallerTypeAPI)))

	// namespaceB has its own bucket, unaffected by namespaceA's demotion.
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceB", headers.CallerTypeAPI)))

	// namespaceC has fairness off, never demoted.
	for range 50 {
		require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	}

	// Preemptable sinks to band 4 only when fairness is active for the namespace.
	for _, ns := range []string{"namespaceA", "namespaceB"} {
		require.Equal(t, 4, priorityFn(req(apiAddActivity, ns, headers.CallerTypePreemptable)))
	}
	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, 1, priorityFn(req(apiAddActivity, ns, headers.CallerTypePreemptable)))
	}
	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, quotas.OperatorPriority,
			priorityFn(req(apiAddActivity, ns, headers.CallerTypeOperator)))
	}

	// namespaceB demotes independently of namespaceA.
	for range 100 {
		priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI))
	}
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeOperator)))
}

func TestFairnessPriorityFn_DisablingValues(t *testing.T) {
	cases := map[string]float64{
		"zero":      0,
		"negative":  -0.5,
		"one":       1.0,
		"above-one": 1.5,
	}
	for name, fs := range cases {
		t.Run(name, func(t *testing.T) {
			priorityFn, _ := makeTestPriorityFn(t, 100,
				map[string]float64{"namespaceA": fs},
			)
			for range 200 {
				require.Equal(t, 1,
					priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
			}
			require.Equal(t, 2,
				priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))
			require.Equal(t, 1,
				priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypePreemptable)))
			require.Equal(t, quotas.OperatorPriority,
				priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeOperator)))
		})
	}
}

func TestFairnessPriorityFn_PrioritiesIncludeAPIAndExtras(t *testing.T) {
	_, priorities := makeTestPriorityFn(t, 100, nil)
	require.Len(t, priorities, len(configs.APIPrioritiesOrdered)+2)
	require.Equal(t, configs.APIPrioritiesOrdered, priorities[:len(configs.APIPrioritiesOrdered)])
	require.Equal(t, 3, priorities[len(priorities)-2])
	require.Equal(t, 4, priorities[len(priorities)-1])
}

// TestFairnessPriorityFn_DemotionMetric verifies the
// service_requests_namespace_fairness_demoted counter fires once per
// over-share demotion, never for the Preemptable sink path, and never for
// namespaces where fairness is disabled.
func TestFairnessPriorityFn_DemotionMetric(t *testing.T) {
	mh := metricstest.NewCaptureHandler()
	cfg := &Config{
		RPS: func() int { return 100 },
		NamespaceMatchingFairShare: func(ns string) float64 {
			return map[string]float64{"namespaceA": 0.5}[ns]
		},
		OperatorRPSRatio: dynamicconfig.GetFloatPropertyFn(0.2),
	}
	priorityFn, _ := getFairnessPriorityFn(cfg, mh)

	const metricName = "service_requests_namespace_fairness_demoted"

	// In-share: no demotion metric.
	capture := mh.StartCapture()
	for range 50 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	require.Empty(t, capture.Snapshot()[metricName])
	mh.StopCapture(capture)

	// Drain bucket, then count demotions: each subsequent in-share call should
	// emit exactly one demoted sample tagged with namespace and caller_type.
	for range 200 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	capture = mh.StartCapture()
	const overShareCalls = 5
	for range overShareCalls {
		require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	}
	samples := capture.Snapshot()[metricName]
	require.Len(t, samples, overShareCalls)
	for _, s := range samples {
		require.Equal(t, int64(1), s.Value)
		require.Equal(t, "namespaceA", s.Tags["namespace"])
		require.Equal(t, headers.CallerTypeAPI, s.Tags["caller_type"])
	}
	mh.StopCapture(capture)

	// Preemptable sink does not emit (bucket is bypassed entirely).
	capture = mh.StartCapture()
	for range 10 {
		require.Equal(t, 4, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypePreemptable)))
	}
	require.Empty(t, capture.Snapshot()[metricName])
	mh.StopCapture(capture)

	// Fairness-disabled namespace (no share) does not emit.
	capture = mh.StartCapture()
	for range 10 {
		priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI))
	}
	require.Empty(t, capture.Snapshot()[metricName])
	mh.StopCapture(capture)
}
