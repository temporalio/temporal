package faultinjection

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestConfigEnabled(t *testing.T) {
	require.False(t, Config{}.Enabled())
	require.False(t, Config{Rate: 0}.Enabled())
	require.True(t, Config{Rate: 0.5}.Enabled())
	require.True(t, Config{ErrorRate: 0.5}.Enabled()) // error fault alone enables the injector
}

func TestMaybeError_DisabledReturnsNil(t *testing.T) {
	i := New(Config{ErrorRate: 0})
	for range 100 {
		require.NoError(t, i.maybeError("/some.Service/Method"))
	}
}

func TestMaybeError_AlwaysReturnsRetryableError(t *testing.T) {
	i := New(Config{ErrorRate: 1.0, Seed: 9})
	for range 100 {
		err := i.maybeError("/some.Service/Method")
		require.Error(t, err)
		code := status.Code(err)
		require.Contains(t, []codes.Code{codes.Unavailable, codes.ResourceExhausted}, code)
	}
}

func TestMaybeError_ExcludedMethod(t *testing.T) {
	i := New(Config{ErrorRate: 1.0})
	require.NoError(t, i.maybeError("/grpc.health.v1.Health/Check"))
}

func TestMaybeError_Rate(t *testing.T) {
	i := New(Config{ErrorRate: 0.5, Seed: 3})
	const n = 20000
	errs := 0
	for range n {
		if i.maybeError("/some.Service/Method") != nil {
			errs++
		}
	}
	require.InDelta(t, 0.5, float64(errs)/n, 0.02)
}

func TestUnaryInterceptor_InjectsTransientError(t *testing.T) {
	i := New(Config{ErrorRate: 1.0, Seed: 5})
	called := false
	handler := func(ctx context.Context, req any) (any, error) { called = true; return "ok", nil }
	_, err := i.UnaryServerInterceptor()(
		context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/some.Service/Method"}, handler,
	)
	require.Error(t, err)
	require.Contains(t, []codes.Code{codes.Unavailable, codes.ResourceExhausted}, status.Code(err))
	require.False(t, called, "handler must not run when a transient error is injected")
}

func TestDisabledNeverFaults(t *testing.T) {
	i := New(Config{Rate: 0, Latency: LatencyConfig{MeanMs: 100}})
	for range 100 {
		require.False(t, i.shouldFault("/some.Service/Method"))
	}
}

func TestExcludedMethods(t *testing.T) {
	i := New(Config{Rate: 1.0})
	require.False(t, i.shouldFault("/grpc.health.v1.Health/Check"))
	require.True(t, i.shouldFault("/some.Service/Method"))
}

func TestMethodAllowList(t *testing.T) {
	i := New(Config{Rate: 1.0, Methods: []string{"HistoryService"}})
	require.True(t, i.shouldFault("/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution"))
	require.False(t, i.shouldFault("/temporal.api.workflowservice.v1.WorkflowService/StartWorkflowExecution"))
}

func TestSeededDeterminism(t *testing.T) {
	cfg := Config{Rate: 0.5, Seed: 42, Latency: LatencyConfig{MeanMs: 50, StddevMs: 20}}
	a, b := New(cfg), New(cfg)
	for range 100 {
		require.Equal(t, a.shouldFault("/some.Service/Method"), b.shouldFault("/some.Service/Method"))
		require.Equal(t, a.sampleLatency(), b.sampleLatency())
	}
}

func TestSampleLatencyClamped(t *testing.T) {
	i := New(Config{Rate: 1.0, Seed: 1, Latency: LatencyConfig{MeanMs: 0, StddevMs: 1000, MaxMs: 100}})
	for range 1000 {
		d := i.sampleLatency()
		require.GreaterOrEqual(t, d, time.Duration(0))
		require.LessOrEqual(t, d, 100*time.Millisecond)
	}
}

func TestSampleLatencyDistribution(t *testing.T) {
	i := New(Config{Rate: 1.0, Seed: 7, Latency: LatencyConfig{MeanMs: 50, StddevMs: 10}})
	const n = 20000
	var sum, sumSq float64
	for range n {
		ms := float64(i.sampleLatency()) / float64(time.Millisecond)
		sum += ms
		sumSq += ms * ms
	}
	mean := sum / n
	stddev := math.Sqrt(sumSq/n - mean*mean)
	require.InDelta(t, 50, mean, 1.0)
	require.InDelta(t, 10, stddev, 1.0)
}

func TestRateFraction(t *testing.T) {
	i := New(Config{Rate: 0.5, Seed: 3})
	const n = 20000
	faulted := 0
	for range n {
		if i.shouldFault("/some.Service/Method") {
			faulted++
		}
	}
	require.InDelta(t, 0.5, float64(faulted)/n, 0.02)
}

func TestInjectRespectsCancelledContext(t *testing.T) {
	i := New(Config{Rate: 1.0, Latency: LatencyConfig{MeanMs: 10000, StddevMs: 0}})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	err := i.inject(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), time.Second)
}

func TestUnaryInterceptorPassesThroughWhenDisabled(t *testing.T) {
	i := New(Config{Rate: 0})
	called := false
	handler := func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	}
	resp, err := i.UnaryServerInterceptor()(
		context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/some.Service/Method"}, handler,
	)
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
	require.True(t, called)
}

func TestUnaryInterceptorInjectsLatency(t *testing.T) {
	i := New(Config{Rate: 1.0, Seed: 5, Latency: LatencyConfig{MeanMs: 30, StddevMs: 0}})
	handler := func(ctx context.Context, req any) (any, error) { return "ok", nil }
	start := time.Now()
	resp, err := i.UnaryServerInterceptor()(
		context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/some.Service/Method"}, handler,
	)
	require.NoError(t, err)
	require.Equal(t, "ok", resp)
	require.GreaterOrEqual(t, time.Since(start), 25*time.Millisecond)
}
