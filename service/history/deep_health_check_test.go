package history

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
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
		desc                string
		timeSinceStartup    time.Duration
		percentilesEnforced bool
		grpcHealthStatus    healthpb.HealthCheckResponse_ServingStatus
		healthCheckSettings health2.Settings
		rpcMethod           string
		historyRecords      []record
		persistRecords      []record
		expected            *historyservice.DeepHealthCheckResponse
		shouldError         bool
		expectedError       string
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					// signal aggregator overall bucket, fed by the records above
					{
						CheckType: health2.CheckTypeRPCLatencyOverall + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "history service overall percentile latency (P99.00 < 2000ms, enforced: true)",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatioOverall,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "history service overall error ratio (< 0.10, enforced: true)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
				State: enumsspb.HEALTH_STATE_NOT_SERVING,
				Checks: []*healthspb.HealthCheck{
					{
						CheckType: health2.CheckTypeGRPCHealth,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Message:   "historyservice gRPC health check: SERVING",
					},
					{
						CheckType: health2.CheckTypeRPCLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					// over the 500ms threshold, but the legacy percentiles are not enforced
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatio,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "historyservice error ratio",
					},
					{
						CheckType: health2.CheckTypeRPCLatencyOverall + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     900,
						Threshold: 2000,
						Message:   "history service overall percentile latency (P99.00 < 2000ms, enforced: true)",
					},
					// the group is enforced, so this is what drives the overall NOT_SERVING
					{
						CheckType: health2.CheckTypeRPCLatencyGroup + "_critical_P99.00",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     900,
						Threshold: 200,
						Message:   "history service critical group percentile latency (P99.00 < 200ms, enforced: true)",
					},
					{
						CheckType: health2.CheckTypeRPCErrorRatioGroup + "_critical",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 0.1,
						Message:   "history service critical group error ratio (< 0.10, enforced: true)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency,
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
				{1500 * time.Millisecond, nil},
				{1500 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{800 * time.Millisecond, context.DeadlineExceeded},
				{800 * time.Millisecond, context.DeadlineExceeded},
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
						Value:     1500,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     1500,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     1500,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     1500,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						Value:     800,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     800,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     800,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     800,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
			desc:                "rpc latency and persistence error ratio over thresholds, percentiles enforced",
			timeSinceStartup:    5 * time.Minute,
			percentilesEnforced: true,
			grpcHealthStatus:    healthpb.HealthCheckResponse_SERVING,
			historyRecords: []record{
				{1500 * time.Millisecond, nil},
				{1500 * time.Millisecond, nil},
			},
			persistRecords: []record{
				{800 * time.Millisecond, context.DeadlineExceeded},
				{800 * time.Millisecond, context.DeadlineExceeded},
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
						Value:     1500,
						Threshold: 1000,
						Message:   "historyservice latency",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     1500,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: true)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     1500,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: true)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     1500,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: true)",
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
						Value:     800,
						Threshold: 1000,
						Message:   "persistenceservice latency",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     800,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: true)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     800,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: true)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_NOT_SERVING,
						Value:     800,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: true)",
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     2000,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     2000,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     2000,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     100,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypeRPCLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 2000,
						Message:   "historyservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 1000,
						Message:   "historyservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypeRPCLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 500,
						Message:   "historyservice percentile latency (P50.00 < 500, enforced: false)",
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
						CheckType: health2.CheckTypePersistenceLatency + "_P99.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 2000,
						Message:   "persistenceservice percentile latency (P99.00 < 2000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P90.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 1000,
						Message:   "persistenceservice percentile latency (P90.00 < 1000, enforced: false)",
					},
					{
						CheckType: health2.CheckTypePersistenceLatency + "_P50.00",
						State:     enumsspb.HEALTH_STATE_SERVING,
						Value:     0,
						Threshold: 500,
						Message:   "persistenceservice percentile latency (P50.00 < 500, enforced: false)",
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
					HealthCheckHistoryGRPCSettings:  func() health2.Settings { return tc.healthCheckSettings },
					HealthRPCLatencyPercentiles: func() dynamicconfig.LatencyHealthChecksPerPercentile {
						return dynamicconfig.LatencyHealthChecksPerPercentile{
							PercentileSettings: []dynamicconfig.LatencyHealthCheckSettings{
								{
									Percentile: 0.99,
									Threshold:  2 * time.Second,
									Enforced:   tc.percentilesEnforced,
								},
								{
									Percentile: 0.90,
									Threshold:  1 * time.Second,
									Enforced:   tc.percentilesEnforced,
								},
								{
									Percentile: 0.50,
									Threshold:  500 * time.Millisecond,
									Enforced:   tc.percentilesEnforced,
								},
							},
						}
					},
					HealthPersistenceLatencyPercentiles: func() dynamicconfig.LatencyHealthChecksPerPercentile {
						return dynamicconfig.LatencyHealthChecksPerPercentile{
							PercentileSettings: []dynamicconfig.LatencyHealthCheckSettings{
								{
									Percentile: 0.99,
									Threshold:  2 * time.Second,
									Enforced:   tc.percentilesEnforced,
								},
								{
									Percentile: 0.90,
									Threshold:  1 * time.Second,
									Enforced:   tc.percentilesEnforced,
								},
								{
									Percentile: 0.50,
									Threshold:  500 * time.Millisecond,
									Enforced:   tc.percentilesEnforced,
								},
							},
						}
					},
				},
				historyHealthSignal:     interceptor.NewHealthSignalAggregator(testLogger, func() bool { return true }, func() bool { return true }, func() health2.Settings { return tc.healthCheckSettings }, time.Second, 10),
				persistenceHealthSignal: persistence.NewHealthSignalAggregator(true, func() bool { return true }, time.Second, 100, metrics.NoopMetricsHandler, testLogger, time.Second, 10),
				startupTime:             startupTime,
			}

			handler.healthServer.SetServingStatus(serviceName, tc.grpcHealthStatus)

			for _, r := range tc.historyRecords {
				handler.historyHealthSignal.Record(tc.rpcMethod, r.latency, r.err)
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
