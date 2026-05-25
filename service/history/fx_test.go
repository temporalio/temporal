package history

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/nettest"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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
	require.Equal(t, []int{0, 1, 2, 3, 4, 5}, priorities)

	// In-share API request from each namespace should yield caller-type
	// priorities. namespaceC falls through because no share is configured.
	require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceC", headers.CallerTypeAPI)))

	// Drain namespaceA's bucket. With share=100 and burst=200, after ~250
	// in-share calls at frozen time the bucket is empty.
	for range 250 {
		priorityFn(req("namespaceA", headers.CallerTypeAPI))
	}
	// namespaceA is now over-share. Every caller type except Preemptable
	// collapses into the single over-share band at priority 4 — including
	// Operator, which now participates in the fairness check.
	require.Equal(t, 4, priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 4, priorityFn(req("namespaceA", headers.CallerTypeOperator)))
	require.Equal(t, 4, priorityFn(req("namespaceA", headers.CallerTypeBackgroundHigh)))
	require.Equal(t, 4, priorityFn(req("namespaceA", headers.CallerTypeBackgroundLow)))

	// Beta has its own bucket and is unaffected by namespaceA's demotion: still
	// in-share at its caller-type priority.
	require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
		priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeBackgroundHigh],
		priorityFn(req("namespaceB", headers.CallerTypeBackgroundHigh)))

	// Gamma has no configured share -> never demoted, keeps caller-type pri.
	for range 50 {
		require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
			priorityFn(req("namespaceC", headers.CallerTypeAPI)))
	}

	// Preemptable always sinks to the bottom (priority 5) regardless of
	// fairness state, namespace share, or how drained the bucket is — it's
	// exempt from the fairness check entirely.
	for _, ns := range []string{"namespaceA", "namespaceB", "namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, 5, priorityFn(req(ns, headers.CallerTypePreemptable)),
			"preemptable from caller=%q should be priority 5", ns)
	}
	// Operator from a namespace whose fairness check is skipped (no configured
	// share, empty caller, or system caller) keeps its caller-type priority 0.
	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, 0, priorityFn(req(ns, headers.CallerTypeOperator)),
			"operator from skip-case caller=%q should stay at priority 0", ns)
	}

	// Drain namespaceB's bucket too and verify it now demotes (independent of
	// namespaceA), and that all over-share caller types collapse to priority 4.
	for range 200 {
		priorityFn(req("namespaceB", headers.CallerTypeAPI))
	}
	require.Equal(t, 4, priorityFn(req("namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 4, priorityFn(req("namespaceB", headers.CallerTypeOperator)))
	require.Equal(t, 4, priorityFn(req("namespaceB", headers.CallerTypeBackgroundLow)))
}

// TestFairnessPriorityFn_DisabledFlagFallsThrough verifies that with the
// kill switch off, every caller type maps to its legacy caller-type
// priority — no demotion, no Preemptable bottom-band override.
func TestFairnessPriorityFn_DisabledFlagFallsThrough(t *testing.T) {
	priorityFn, _ := makeTestPriorityFn(t,
		false, // fairness disabled
		1.0, 1.0, true,
		map[string]int{"namespaceA": 1}, // tiny share; would demote if enabled
	)
	// Drain what would be the bucket — with fairness off, no demotion
	// happens regardless.
	for callerType, expected := range configs.CallerTypeToPriority {
		for range 50 {
			require.Equal(t, expected, priorityFn(req("namespaceA", callerType)),
				"callerType=%s with fairness disabled should keep its priority", callerType)
		}
	}
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
	for range 50 {
		require.Equal(t, configs.CallerTypeToPriority[headers.CallerTypeAPI],
			priorityFn(req("namespaceA", headers.CallerTypeAPI)))
	}
}

type testHistoryService struct {
	historyservice.UnimplementedHistoryServiceServer
}

func (testHistoryService) StartWorkflowExecution(
	context.Context,
	*historyservice.StartWorkflowExecutionRequest,
) (*historyservice.StartWorkflowExecutionResponse, error) {
	return &historyservice.StartWorkflowExecutionResponse{}, nil
}

// TestRateLimitInterceptor_FairnessEndToEnd drives real gRPC requests through
// the production RateLimitInterceptor. Both namespaces share the same fair
// share; hot bursts past its share, cold stays within. Asserts hot is
// throttled and cold is not.
func TestRateLimitInterceptor_FairnessEndToEnd(t *testing.T) {
	lazy := shard.LazyLoadedOwnershipBasedQuotaScaler{Value: &atomic.Value{}}
	lazy.Store(shard.OwnershipBasedQuotaScaler(stubScaler{factor: 1.0, ok: true}))
	cfg := &configs.Config{
		RPS:                          func() int { return 100 },
		EnableNamespaceFairness:      func() bool { return true },
		NamespaceFairShareMultiplier: func() float64 { return 1.0 },
		FrontendGlobalNamespaceRPS:   func(string) int { return 10 },
		OperatorRPSRatio:             func() float64 { return 0.2 },
	}
	rl := RateLimitInterceptorProvider(cfg, lazy, metrics.NoopMetricsHandler)

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(rl.Intercept))
	historyservice.RegisterHistoryServiceServer(server, testHistoryService{})
	pipe := nettest.NewPipe()
	listener := nettest.NewListener(pipe)

	var wg sync.WaitGroup
	wg.Go(func() {
		_ = server.Serve(listener)
	})
	t.Cleanup(func() {
		server.Stop()
		wg.Wait()
	})

	conn, err := grpc.NewClient("localhost",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return pipe.Connect(ctx.Done())
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	conn.Connect()
	client := historyservice.NewHistoryServiceClient(conn)

	callerCtx := func(name, callerType string) context.Context {
		return metadata.AppendToOutgoingContext(context.Background(),
			headers.CallerNameHeaderName, name,
			headers.CallerTypeHeaderName, callerType,
		)
	}
	send := func(ns, callerType string) error {
		_, err := client.StartWorkflowExecution(callerCtx(ns, callerType),
			&historyservice.StartWorkflowExecutionRequest{})
		return err
	}

	allCallerTypes := []string{
		headers.CallerTypeOperator,
		headers.CallerTypeAPI,
		headers.CallerTypeBackgroundHigh,
		headers.CallerTypeBackgroundLow,
		headers.CallerTypePreemptable,
	}
	// Preemptable always lands in the bottom band globally in history's
	// design, so excluded from cold's per-namespace isolation check.
	coldCallerTypes := []string{
		headers.CallerTypeOperator,
		headers.CallerTypeAPI,
		headers.CallerTypeBackgroundHigh,
		headers.CallerTypeBackgroundLow,
	}

	for _, ct := range allCallerTypes {
		for range 60 {
			_ = send("hot_namespace", ct)
		}
	}

	type result struct {
		ns         string
		callerType string
		err        error
	}
	var results []result
	for range 2 {
		for _, ct := range allCallerTypes {
			results = append(results, result{"hot_namespace", ct, send("hot_namespace", ct)})
		}
		for _, ct := range coldCallerTypes {
			results = append(results, result{"cold_namespace", ct, send("cold_namespace", ct)})
		}
	}

	var hotRejected, coldRejected int
	for _, r := range results {
		if r.err == nil {
			continue
		}
		if r.ns == "hot_namespace" {
			hotRejected++
			continue
		}
		coldRejected++
		t.Errorf("unexpected rejection from cold_namespace caller_type=%s: %v",
			r.callerType, r.err)
	}
	require.Positive(t, hotRejected, "hot_namespace should be throttled")
	require.Zero(t, coldRejected, "cold_namespace should not be throttled")
}
