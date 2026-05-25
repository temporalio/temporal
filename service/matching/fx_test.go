package matching

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/nettest"
	"go.temporal.io/server/service/matching/configs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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
	nsFairShare := map[string]float64{
		"namespaceA": 0.5,
		"namespaceB": 0.2,
	}
	priorityFn, priorities := makeTestPriorityFn(t, 100, nsFairShare)
	require.Equal(t, []int{0, 1, 2, 3}, priorities)

	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))

	for range 150 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeOperator)))
	require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypePreemptable)))
	require.Equal(t, 3, priorityFn(req(apiCancelPolls, "namespaceA", headers.CallerTypeAPI)))
	require.Equal(t, 3, priorityFn(req(apiUnknownMethod, "namespaceA", headers.CallerTypeAPI)))

	require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI)))
	require.Equal(t, 2, priorityFn(req(apiCancelPolls, "namespaceB", headers.CallerTypeAPI)))

	for range 50 {
		require.Equal(t, 1, priorityFn(req(apiAddActivity, "namespaceC", headers.CallerTypeAPI)))
	}

	for _, ns := range []string{"namespaceC", "", headers.CallerNameSystem} {
		require.Equal(t, 1, priorityFn(req(apiAddActivity, ns, headers.CallerTypePreemptable)))
		require.Equal(t, quotas.OperatorPriority,
			priorityFn(req(apiAddActivity, ns, headers.CallerTypeOperator)))
	}

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
	require.Len(t, priorities, len(configs.APIPrioritiesOrdered)+1)
	require.Equal(t, configs.APIPrioritiesOrdered, priorities[:len(configs.APIPrioritiesOrdered)])
	require.Equal(t, 3, priorities[len(priorities)-1])
}

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

	capture := mh.StartCapture()
	for range 50 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	require.Empty(t, capture.Snapshot()[metrics.ServiceRequestsNamespaceFairnessDemoted.Name()])
	mh.StopCapture(capture)

	for range 200 {
		priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI))
	}
	capture = mh.StartCapture()
	const overShareCalls = 5
	for range overShareCalls {
		require.Equal(t, 3, priorityFn(req(apiAddActivity, "namespaceA", headers.CallerTypeAPI)))
	}
	samples := capture.Snapshot()[metrics.ServiceRequestsNamespaceFairnessDemoted.Name()]
	require.Len(t, samples, overShareCalls)
	for _, s := range samples {
		require.Equal(t, int64(1), s.Value)
		require.Equal(t, "namespaceA", s.Tags["namespace"])
		require.Equal(t, headers.CallerTypeAPI, s.Tags["caller_type"])
	}
	mh.StopCapture(capture)

	capture = mh.StartCapture()
	for range 10 {
		priorityFn(req(apiAddActivity, "namespaceB", headers.CallerTypeAPI))
	}
	require.Empty(t, capture.Snapshot()[metrics.ServiceRequestsNamespaceFairnessDemoted.Name()])
	mh.StopCapture(capture)
}

type testMatchingService struct {
	matchingservice.UnimplementedMatchingServiceServer
}

func (testMatchingService) AddActivityTask(
	context.Context,
	*matchingservice.AddActivityTaskRequest,
) (*matchingservice.AddActivityTaskResponse, error) {
	return &matchingservice.AddActivityTaskResponse{}, nil
}

// TestRateLimitInterceptor_FairnessEndToEnd drives real gRPC requests through
// the RateLimitInterceptor. Both namespaces share the same fair share;
// hot bursts past its share, cold stays within. Asserts hot is throttled and cold is not.
func TestRateLimitInterceptor_FairnessEndToEnd(t *testing.T) {
	cfg := &Config{
		RPS:                        func() int { return 100 },
		NamespaceMatchingFairShare: func(string) float64 { return 0.1 },
		OperatorRPSRatio:           dynamicconfig.GetFloatPropertyFn(0.2),
	}
	rl := RateLimitInterceptorProvider(cfg, metrics.NoopMetricsHandler)

	server := grpc.NewServer(grpc.ChainUnaryInterceptor(rl.Intercept))
	matchingservice.RegisterMatchingServiceServer(server, testMatchingService{})
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
	client := matchingservice.NewMatchingServiceClient(conn)

	callerCtx := func(name, callerType string) context.Context {
		return metadata.AppendToOutgoingContext(context.Background(),
			headers.CallerNameHeaderName, name,
			headers.CallerTypeHeaderName, callerType,
		)
	}
	send := func(ns, callerType string) error {
		_, err := client.AddActivityTask(callerCtx(ns, callerType),
			&matchingservice.AddActivityTaskRequest{})
		return err
	}

	callerTypes := []string{
		headers.CallerTypeOperator,
		headers.CallerTypeAPI,
		headers.CallerTypeBackgroundHigh,
		headers.CallerTypeBackgroundLow,
		headers.CallerTypePreemptable,
	}

	for _, ct := range callerTypes {
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
		for _, ct := range callerTypes {
			results = append(results, result{"hot_namespace", ct, send("hot_namespace", ct)})
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
