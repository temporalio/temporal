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
	"go.temporal.io/server/common/stats"
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
		desc                           string
		timeSinceStartup               time.Duration
		grpcHealthStatus               healthpb.HealthCheckResponse_ServingStatus
		healthCheckSettings            health2.Settings
		persistenceHealthCheckSettings health2.Settings
		rpcMethod                      string
		historyRecords                 []record
		persistRecords                 []record
		expected                       *historyservice.DeepHealthCheckResponse
		shouldError                    bool
		expectedError                  string
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
				State:           enumsspb.HEALTH_STATE_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
				},
			},
		},
		{
			desc:             "overall health check settings thresholds satisfied",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			healthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds:  []health2.QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					ErrorRatioThreshold: &health2.ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
					Enforced:            true,
				},
			},
			rpcMethod: "/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
			historyRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State:           enumsspb.HEALTH_STATE_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
					// signal aggregator overall bucket, fed by the records above
					{
						CheckType: "history.grpc.overall.latency.p99",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Enforced:  true,
					},
					{
						CheckType: "history.grpc.overall.error_ratio",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Enforced:  true,
					},
				},
			},
		},
		{
			desc:             "group latency over threshold while overall stays healthy",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			healthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []health2.QuantileThreshold{{Quantile: 0.99, Threshold: 2 * time.Second}},
					Enforced:           true,
				},
				Groups: []health2.Group{
					{
						Name: "critical",
						Keys: []string{"/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution"},
						Thresholds: health2.Thresholds{
							WindowConfig:        &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
							QuantileThresholds:  []health2.QuantileThreshold{{Quantile: 0.99, Threshold: 200 * time.Millisecond}},
							ErrorRatioThreshold: &health2.ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
							Enforced:            true,
						},
					},
				},
			},
			rpcMethod: "/temporal.server.api.historyservice.v1.HistoryService/StartWorkflowExecution",
			// 900ms is under the overall 2s threshold but well over the group's 200ms
			historyRecords: []record{
				{900 * time.Millisecond, nil},
				{900 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State:           enumsspb.HEALTH_STATE_NOT_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
					// over the 500ms threshold, but the legacy percentiles are not enforced
					{
						CheckType: "history.grpc.overall.latency.p99",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 2000,
						Enforced:  true,
					},
					// the group is enforced, so this is what drives the overall NOT_SERVING
					{
						CheckType: "history.grpc.group.critical.latency.p99",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     900,
						Threshold: 200,
						Enforced:  true,
					},
					{
						CheckType: "history.grpc.group.critical.error_ratio",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Enforced:  true,
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
				State:           enumsspb.HEALTH_STATE_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: NOT_SERVING",
						Enforced:  true,
					},
				},
			},
		},
		{
			desc:             "grpc latency and persistence error ratio over thresholds",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			healthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []health2.QuantileThreshold{{Quantile: 0.99, Threshold: time.Second}},
					Enforced:           true,
				},
			},
			persistenceHealthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					ErrorRatioThreshold: &health2.ErrorRatioThreshold{WindowSize: 10 * time.Second, BufferSize: 5000, Threshold: 0.1},
					Enforced:            true,
				},
			},
			historyRecords: []record{
				{1500 * time.Millisecond, nil},
				{1500 * time.Millisecond, nil},
			},
			// deadline exceeded counts as unhealthy, so every call is an error
			persistRecords: []record{
				{800 * time.Millisecond, context.DeadlineExceeded},
				{800 * time.Millisecond, context.DeadlineExceeded},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State:           enumsspb.HEALTH_STATE_NOT_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
					{
						CheckType: "history.grpc.overall.latency.p99",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     1500,
						Threshold: 1000,
						Enforced:  true,
					},
					{
						CheckType: "history.persistence.overall.error_ratio",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     1,
						Threshold: 0.1,
						Enforced:  true,
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
				State:           enumsspb.HEALTH_STATE_NOT_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Message:   "historyservice gRPC health check: NOT_SERVING",
						Enforced:  true,
					},
				},
			},
		},
		{
			desc:             "init window does not suppress threshold checks",
			timeSinceStartup: 30 * time.Second,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			healthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []health2.QuantileThreshold{{Quantile: 0.99, Threshold: time.Second}},
					Enforced:           true,
				},
			},
			historyRecords: []record{
				{2 * time.Second, nil},
				{2 * time.Second, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State:           enumsspb.HEALTH_STATE_NOT_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
					// the init window only suppresses the gRPC health status, not thresholds
					{
						CheckType: "history.grpc.overall.latency.p99",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     2000,
						Threshold: 1000,
						Enforced:  true,
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
				State:           enumsspb.HEALTH_STATE_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
				},
			},
		},
		{
			desc:             "unenforced check over threshold only moves the unenforced state",
			timeSinceStartup: 5 * time.Minute,
			grpcHealthStatus: healthpb.HealthCheckResponse_SERVING,
			healthCheckSettings: health2.Settings{
				Overall: health2.Thresholds{
					WindowConfig:       &stats.WindowConfig{WindowSize: 5 * time.Second, WindowCount: 10},
					QuantileThresholds: []health2.QuantileThreshold{{Quantile: 0.99, Threshold: 500 * time.Millisecond}},
					Enforced:           false,
				},
			},
			// 600ms is over the threshold, but the overall bucket is not enforced
			historyRecords: []record{
				{600 * time.Millisecond, nil},
				{600 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{100 * time.Millisecond, nil},
				{100 * time.Millisecond, nil},
			},
			expected: &historyservice.DeepHealthCheckResponse{
				State:           enumsspb.HEALTH_STATE_SERVING,
				UnenforcedState: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
						Enforced:  true,
					},
					// the only breached check, and it is unenforced
					{
						CheckType: "history.grpc.overall.latency.p99",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     600,
						Threshold: 500,
						Enforced:  false,
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
					HealthHistoryInitializationTime:  func() time.Duration { return time.Minute },
					HealthHistoryGRPCSettings:        func() health2.Settings { return tc.healthCheckSettings },
					HealthHistoryPersistenceSettings: func() health2.Settings { return tc.persistenceHealthCheckSettings },
				},
				historyHealthSignal:     interceptor.NewHealthSignalAggregator(testLogger, func() bool { return true }, func() health2.Settings { return tc.healthCheckSettings }, time.Second, 10),
				persistenceHealthSignal: persistence.NewHealthSignalAggregator(true, time.Second, 100, metrics.NoopMetricsHandler, testLogger, func() health2.Settings { return health2.Settings{} }),
				startupTime:             startupTime,
			}

			handler.healthServer.SetServingStatus(serviceName, tc.grpcHealthStatus)

			for _, r := range tc.historyRecords {
				handler.historyHealthSignal.Record(tc.rpcMethod, r.latency, r.err)
			}

			for _, r := range tc.persistRecords {
				handler.persistenceHealthSignal.Record(metrics.PersistenceGetWorkflowExecutionScope, 1, r.latency, r.err)
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
