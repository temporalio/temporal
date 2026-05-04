package history

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
)

// stubScaler implements shard.OwnershipBasedQuotaScaler with a fixed
// scale factor.
type stubScaler struct {
	factor float64
	ok     bool
}

func (s stubScaler) ScaleFactor() (float64, bool) { return s.factor, s.ok }

// makeTestPriorityFn builds the production priority function used by
// RateLimitInterceptorProvider, with a stubbed scale factor and per-namespace
// frontend RPS map.
func makeTestPriorityFn(
	t *testing.T,
	enabled bool,
	multiplier float64,
	scale float64,
	scaleOk bool,
	nsRPS map[string]int,
) (quotas.RequestPriorityFn, []int) {
	t.Helper()
	lazy := shard.LazyLoadedOwnershipBasedQuotaScaler{Value: &atomic.Value{}}
	lazy.Store(shard.OwnershipBasedQuotaScaler(stubScaler{factor: scale, ok: scaleOk}))
	cfg := &configs.Config{
		EnableNamespaceFairness:      func() bool { return enabled },
		NamespaceFairShareMultiplier: func() float64 { return multiplier },
		FrontendGlobalNamespaceRPS: func(ns string) int {
			return nsRPS[ns]
		},
	}
	return getFairnessPriorityFn(cfg, lazy, metrics.NoopMetricsHandler)
}

func req(caller, callerType string) quotas.Request {
	return quotas.NewRequest("/some/method", 1, caller, callerType, 0, "")
}

// TestFairnessPriorityFn_MultipleCallers exercises the end-to-end behavior
// across multiple namespaces simultaneously: each namespace has its own
// fair-share bucket, over-share traffic from one namespace gets demoted
// while in-share traffic from another stays at its caller-type priority,
// and Preemptable always lands at the lowest priority regardless of
// namespace state.
func TestFairnessPriorityFn_MultipleCallers(t *testing.T) {
	// Three namespaces:
	//   namespaceA has the largest configured share (100 RPS, burst = 200).
	//   namespaceB has a smaller share (50 RPS, burst = 100).
	//   namespaceC has no configured share -> fairness skipped.
	nsRPS := map[string]int{
		"namespaceA": 100,
		"namespaceB": 50,
	}
	priorityFn, priorities := makeTestPriorityFn(t,
		true,  // fairness enabled
		1.0,   // multiplier
		1.0,   // host owns 100% of shards in this test
		true,  // scaler ready
		nsRPS, // per-namespace global RPS
	)
	require.Equal(t, []int{0, 1, 2, 3, 4, 5, 6}, priorities)

	// In-share API request from each namespace should yield caller-type
	// priorities. namespaceC falls through because no share is configured.
	assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceC", headers.CallerTypeAPI)))

	// Drain namespaceA's bucket. With share=100 and burst=200, after ~250
	// in-share calls at frozen time the bucket is empty.
	for i := 0; i < 250; i++ {
		priorityFn(req("namespaceA", headers.CallerTypeAPI))
	}
	// namespaceA is now over-share. API is demoted to priority 4 but Operator
	// is exempt from fairness and stays at priority 0 regardless.
	assert.Equal(t, 4, priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	assert.Equal(t, 0, priorityFn(req("namespaceA", headers.CallerTypeOperator)))
	// namespaceA is over-share for background traffic -> priority 5.
	assert.Equal(t, 5, priorityFn(req("namespaceA", headers.CallerTypeBackgroundHigh)))
	assert.Equal(t, 5, priorityFn(req("namespaceA", headers.CallerTypeBackgroundLow)))

	// Beta has its own bucket and is unaffected by namespaceA's demotion: still
	// in-share at its caller-type priority.
	assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeBackgroundHigh],
		priorityFn(req("namespaceB", headers.CallerTypeBackgroundHigh)))

	// Gamma has no configured share -> never demoted, keeps caller-type pri.
	for i := 0; i < 50; i++ {
		assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
			priorityFn(req("namespaceC", headers.CallerTypeAPI)))
	}

	// Operator always stays at priority 0, and Preemptable always at 6,
	// regardless of fairness state, namespace share, or how drained the
	// bucket is.
	for _, ns := range []string{"namespaceA", "namespaceB", "namespaceC", "", headers.CallerNameSystem} {
		assert.Equal(t, 0, priorityFn(req(ns, headers.CallerTypeOperator)),
			"operator from caller=%q should be priority 0", ns)
		assert.Equal(t, 6, priorityFn(req(ns, headers.CallerTypePreemptable)),
			"preemptable from caller=%q should be priority 6", ns)
	}

	// Drain namespaceB's bucket too and verify it now demotes (independent of
	// namespaceA).
	for i := 0; i < 200; i++ {
		priorityFn(req("namespaceB", headers.CallerTypeAPI))
	}
	assert.Equal(t, 4, priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	assert.Equal(t, 5, priorityFn(req("namespaceB", headers.CallerTypeBackgroundLow)))
}

// TestFairnessPriorityFn_DisabledFlagFallsThrough verifies that with the
// kill switch off, no namespace traffic is demoted even at very low share.
// Preemptable still goes to priority 6 (the absolute-lowest layout applies
// regardless of the flag).
func TestFairnessPriorityFn_DisabledFlagFallsThrough(t *testing.T) {
	priorityFn, _ := makeTestPriorityFn(t,
		false, // fairness disabled
		1.0, 1.0, true,
		map[string]int{"namespaceA": 1}, // tiny share; would demote if enabled
	)
	// Drain what would be the bucket — with fairness off, no demotion
	// happens regardless.
	for callerType, expected := range configs.CallerTypeToPriority {
		if callerType == headers.CallerTypePreemptable {
			continue // tested separately below
		}
		for i := 0; i < 50; i++ {
			assert.Equal(t, expected, priorityFn(req("namespaceA", callerType)),
				"callerType=%s with fairness disabled should keep its priority", callerType)
		}
	}
	// Operator still routes to 0 and Preemptable to 6 unconditionally.
	assert.Equal(t, 0, priorityFn(req("namespaceA", headers.CallerTypeOperator)))
	assert.Equal(t, 6, priorityFn(req("namespaceA", headers.CallerTypePreemptable)))
}

// TestFairnessPriorityFn_ScalerNotReady verifies that when the
// OwnershipBasedQuotaScaler hasn't reported a scale factor yet, no namespace
// is treated as having a positive share, so fairness is skipped.
func TestFairnessPriorityFn_ScalerNotReady(t *testing.T) {
	priorityFn, _ := makeTestPriorityFn(t,
		true,
		1.0, 0, false, // scaler not ready
		map[string]int{"namespaceA": 100},
	)
	for i := 0; i < 50; i++ {
		assert.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
			priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	}
}
