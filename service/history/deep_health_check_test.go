package history

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	health2 "go.temporal.io/server/common/health"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func TestHealthCheck(t *testing.T) {
	testLogger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
	handler := deepHealthCheckHandler{
		healthServer:   health.NewServer(),
		metricsHandler: metrics.NoopMetricsHandler,
		config: &configs.Config{
			// This is in milliseconds
			HealthPersistenceLatencyFailure: func() float64 { return 1000 },
			HealthRPCLatencyFailure:         func() float64 { return 1000 },
			// This is out of 1
			HealthPersistenceErrorRatio: func() float64 { return 0.1 },
			HealthRPCErrorRatio:         func() float64 { return 0.1 },
		},
		historyHealthSignal:     interceptor.NewHealthSignalAggregator(testLogger, func() bool { return true }, time.Second, 100),
		persistenceHealthSignal: persistence.NewHealthSignalAggregator(true, time.Second, 100, metrics.NoopMetricsHandler, testLogger),
		startupTime:             time.Unix(10, 0),
	}
	handler.healthServer.SetServingStatus(serviceName, healthpb.HealthCheckResponse_NOT_SERVING)
	handler.persistenceHealthSignal.Record(1, time.Second, nil)
	handler.historyHealthSignal.Record(time.Second, nil)
	resp, err := handler.DeepHealthCheck(t.Context(), time.Unix(11, 0))
	require.NoError(t, err)
	require.Equalf(t, enumsspb.HEALTH_STATE_SERVING, resp.GetState(), "expected: %s actual: %s",
		enumsspb.HEALTH_STATE_SERVING.String(), resp.GetState().String())
	for _, check := range resp.GetChecks() {
		require.Equalf(t, enumsspb.HEALTH_STATE_SERVING, check.GetState(), "expected: %s actual: %s",
			enumsspb.HEALTH_STATE_SERVING.String(), check.GetState().String())
		switch check.CheckType {
		case health2.CheckTypeGRPCHealth:
			require.True(t, strings.Contains(check.Message, "NOT_SERVING"))
		}
	}
	handler.historyHealthSignal.Record(time.Second, context.DeadlineExceeded)
	resp, err = handler.DeepHealthCheck(t.Context(), time.Unix(11, 0))
	require.NoError(t, err)
	require.Equalf(t, enumsspb.HEALTH_STATE_NOT_SERVING, resp.GetState(), "expected: %s actual: %s",
		enumsspb.HEALTH_STATE_NOT_SERVING.String(), resp.GetState().String())
}
