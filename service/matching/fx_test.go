package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/matching/configs"
)

const (
	apiAddActivity   = "/temporal.server.api.matchingservice.v1.MatchingService/AddActivityTask"
	apiCancelPolls   = "/temporal.server.api.matchingservice.v1.MatchingService/CancelOutstandingWorkerPolls"
	apiUnknownMethod = "/temporal.server.api.matchingservice.v1.MatchingService/SomeUnknownMethod"
)

// makeTestPriorityFn builds the production priority function used by
// RateLimitInterceptorProvider, with a stubbed host RPS and per-namespace
// fair-share map. The map drives both the bucket size and whether fairness
// applies (values outside (0, 1) disable the feature for that namespace).
func makeTestPriorityFn(
	t *testing.T,
	hostRPS int,
	nsFairShare map[string]float64,
) (quotas.RequestPriorityFn, []int) {
	t.Helper()
	cfg := &Config{
		RPS: func() int { return hostRPS },
		NamespaceFairShare: func(ns string) float64 {
			return nsFairShare[ns]
		},
		OperatorRPSRatio: dynamicconfig.GetFloatPropertyFn(0.2),
	}
	return getFairnessPriorityFn(cfg, metrics.NoopMetricsHandler)
}

func req(api, caller, callerType string) quotas.Request {
	return quotas.NewRequest(api, 1, caller, callerType, 0, "")
}

// TestFairnessPriorityFn_MultipleCallers exercises the end-to-end behavior
// across multiple namespaces simultaneously: each namespace has its own
// fair-share bucket sized at MatchingRPS*share(ns), over-share traffic from
// one namespace gets demoted while in-share traffic from another stays at its
// API priority, and Preemptable sinks to the bottom only for namespaces where
// fairness is active.
func TestFairnessPriorityFn_MultipleCallers(t *testing.T) {
	// hostRPS=100, three namespaces:
	//   namespaceA: 50% share -> rate 50 RPS, burst 100. Fairness active.
	//   namespaceB: 20% share -> rate 20 RPS, burst 40.  Fairness active.
	//   namespaceC: 0 (unset) -> fairness disabled for this namespace.
	nsFairShare := map[string]float64{
		"namespaceA": 0.5,
		"namespaceB": 0.2,
	}
	priorityFn, priorities := makeTestPriorityFn(t, 100, nsFairShare)
	require.Equal(t, []int{0, 1, 2, 3, 4}, priorities)

	// In-share API requests from each namespace yield matching API priorities.
	// AddActivityTask is priority 1; CancelOutstandingWorkerPolls is priority 2.
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))

	// Drain namespaceA's bucket. With share=50 and burst=100, after ~150
	// in-share calls at frozen time the bucket is empty.
	for range 150 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	// namespaceA is now over-share. Every caller type except Preemptable
	// collapses into the over-share band at priority 3 — including Operator,
	// which now participates in the fairness check. The API priority no longer
	// matters once we're over-share.
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeOperator)))
	require.Equal(t, 3, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiUnknownMethod, "namespaceA", headers.CallerTypeAPI)))

	// namespaceB has its own bucket and is unaffected by namespaceA's demotion:
	// still in-share at its API priority.
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceB", headers.CallerTypeAPI)))

	// namespaceC has no configured share -> never demoted, keeps API priority.
	for range 50 {
		require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	}

	// Preemptable from namespaces where fairness is active (A and B) sinks to
	// the bottom (priority 4), regardless of bucket state — Preemptable is
	// exempt from the bucket check but lands in the bottom band.
	for _, ns := range []string{"namespaceA", "namespaceB"} {
		require.Equal(t, 4, priorityFn(req(apiAddActivity, ns, headers.CallerTypePreemptable)),
			"preemptable from fairness-active caller=%q should be priority 4", ns)
	}
	// Preemptable from a caller where fairness is disabled (no share, empty
	// caller, or system caller) keeps its API priority — fairness is purely
	// per-namespace and doesn't reach into untracked callers.
	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, 1,
			priorityFn(req(apiAddActivity, ns, headers.CallerTypePreemptable)),
			"preemptable from fairness-disabled caller=%q should keep API priority", ns)
	}
	// Operator from a namespace whose fairness check is skipped (no configured
	// share, empty caller, or system caller) keeps its Operator API priority 0.
	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, quotas.OperatorPriority,
			priorityFn(req(apiAddActivity, ns, headers.CallerTypeOperator)),
			"operator from skip-case caller=%q should stay at priority 0", ns)
	}

	// Drain namespaceB's bucket too and verify it now demotes independently of
	// namespaceA.
	for range 100 {
		priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI))
	}
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeOperator)))
}

// TestFairnessPriorityFn_DisablingValues verifies that the single fair-share
// knob doubles as the on/off switch: values <=0 and >=1 both disable fairness
// for the namespace, so even under heavy traffic the namespace keeps its API
// priority and Preemptable is not demoted.
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
			// Heavy traffic — would saturate a small bucket if fairness were active.
			for range 200 {
				require.Equal(t, 1,
					priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)),
					"API caller should keep API priority when fairness disabled (fairShare=%v)", fs)
			}
			require.Equal(t, 2,
				priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))
			// Preemptable not sunk because fairness is off for this namespace.
			require.Equal(t, 1,
				priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypePreemptable)),
				"preemptable should keep API priority when fairness disabled (fairShare=%v)", fs)
			// Operator keeps its priority.
			require.Equal(t, quotas.OperatorPriority,
				priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeOperator)))
		})
	}
}

// TestFairnessPriorityFn_PrioritiesIncludeAPIAndExtras sanity-checks the
// priority list shape returned alongside the priority fn: existing matching
// API priorities are preserved as the in-share bands, and two new bands
// (over-share, preemptable) are appended at the end.
func TestFairnessPriorityFn_PrioritiesIncludeAPIAndExtras(t *testing.T) {
	_, priorities := makeTestPriorityFn(t, 100, nil)
	require.Len(t, priorities, len(configs.APIPrioritiesOrdered)+2,
		"priorities should be the matching API priorities + 2 fairness bands")
	require.Equal(t, configs.APIPrioritiesOrdered, priorities[:len(configs.APIPrioritiesOrdered)])
	require.Equal(t, 3, priorities[len(priorities)-2], "over-share band")
	require.Equal(t, 4, priorities[len(priorities)-1], "preemptable band")
}
