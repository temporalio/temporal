package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	health2 "go.temporal.io/server/common/health"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestDeepHealthCheck(t *testing.T) {
	type record struct {
		latency time.Duration
		err     error
	}

	testCases := []struct {
		desc             string
		timeSinceStartup time.Duration
		grpcHealthStatus healthpb.HealthCheckResponse_ServingStatus
		historyRecords   []record
		persistRecords   []record
		expected         *historyservice.DeepHealthCheckResponse
		shouldError      bool
		expectedError    string
	}{
		{
			desc:             "all checks healthy",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			historyRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
		{
			desc:             "grpc not_serving suppressed within init window",
			timeSinceStartup: 30 * time.Second,
			grpcHealthStatus: healthpb.HealthCheckResponse_NOT_SERVING,
			historyRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: NOT_SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
		{
			desc:             "rpc latency and persistence error ratio over thresholds",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			historyRecords: []record{
				{2 * time.Second, nil},
				{2 * time.Second, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, context.DeadlineExceeded},
				{100 * time.Millisecond, context.DeadlineExceeded},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     2000,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     1,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
		{
			desc:             "grpc not_serving propagates after init window expires",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_NOT_SERVING,
			historyRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Message:   "historyservice gRPC health check: NOT_SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
		{
			desc:             "init window does not suppress threshold checks",
			timeSinceStartup: 30 * time.Second,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			historyRecords: []record{
				{2 * time.Second, nil},
				{2 * time.Second, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     2000,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
		{
			desc:             "no records reports healthy (aggregator returns 0)",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			historyRecords:   nil,
			persistRecords:   nil,
			expected: &historyservice.DeepHealthCheckResponse{
				State: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceErrRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "persistenceservice error ratio",
					},
				},
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			testLogger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			startupTime := time.Unix(0, 0)

			handler := deepHealthCheckHandler{
				healthServer:   health.NewServer(),
				metricsHandler: metrics.NoopMetricsHandler,
				config: &configs.Config{
					HealthPersistenceLatencyFailure: func() float64 { return 1000 },
					HealthRPCLatencyFailure:         func() float64 { return 1000 },
					HealthPersistenceErrorRatio:     func() float64 { return 0.1 },
					HealthRPCErrorRatio:             func() float64 { return 0.1 },
					HealthHistoryInitializationTime: func() time.Duration { return time.Minute },
				},
				historyHealthSignal:     interceptor.NewHealthSignalAggregator(testLogger, func() bool { return true }, time.Second, 100),
				persistenceHealthSignal: persistence.NewHealthSignalAggregator(true, time.Second, 100, metrics.NoopMetricsHandler, testLogger),
				startupTime:             startupTime,
			}

			handler.healthServer.SetServingStatus(serviceName, tc.grpcHealthStatus)

			for _, r := range tc.historyRecords {
				handler.historyHealthSignal.Record(r.latency, r.err)
			}

			for _, r := range tc.persistRecords {
				handler.persistenceHealthSignal.Record(1, r.latency, r.err)
			}

			actual, err := handler.DeepHealthCheck(t.Context(), startupTime.Add(tc.timeSinceStartup))

			if tc.shouldError && err == nil {
				require.Fail(t, "should have errored but didn't")
			}
			if err != nil {
				require.EqualError(t, err, tc.expectedError)
			}

			require.NoError(t, err)
			require.Equal(t, tc.expected, actual)
		})
	}
}
