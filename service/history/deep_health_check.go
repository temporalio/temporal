package history

import (
	"context"
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	healthcheck "go.temporal.io/server/common/health"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/configs"
	"google.golang.org/grpc/health"
	grpchealthspb "google.golang.org/grpc/health/grpc_health_v1"
)

type deepHealthCheckHandler struct {
	healthServer            *health.Server
	metricsHandler          metrics.Handler
	config                  *configs.Config
	historyHealthSignal     interceptor.HealthSignalAggregator
	persistenceHealthSignal persistence.HealthSignalAggregator
	startupTime             time.Time
}

// DeepHealthCheck implements the grpc API from
// ./proto/internal/temporal/server/api/historyservice/v1/request_response.proto
func (h *deepHealthCheckHandler) DeepHealthCheck(
	ctx context.Context, now time.Time,
) (*historyservice.DeepHealthCheckResponse, error) {
	var checks []*healthspb.HealthCheck

	status, err := h.healthServer.Check(ctx, &grpchealthspb.HealthCheckRequest{Service: serviceName})
	if err != nil || status == nil {
		metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(enumsspb.HEALTH_STATE_NOT_SERVING))
		return &historyservice.DeepHealthCheckResponse{
			State: enumsspb.HEALTH_STATE_NOT_SERVING,
			Checks: []*healthspb.HealthCheck{{
				CheckType: healthcheck.CheckTypeGRPCHealth,
				State:     enumsspb.HEALTH_STATE_NOT_SERVING,
				Message:   fmt.Sprintf("gRPC health check failed: %v", err),
			}},
		}, nil
	}

	checks = append(checks, &healthspb.HealthCheck{
		CheckType: healthcheck.CheckTypeGRPCHealth,
		// Convert to SERVING to avoid false positives during initialization
		State:   suppressStartupErrors(status.Status, now.Sub(h.startupTime), h.config.HealthHistoryInitializationTime()),
		Message: fmt.Sprintf("historyservice gRPC health check: %s", status.Status.String()),
	})

	checks = append(checks, errorIfOverThreshold(healthcheck.CheckTypeRPCLatency,
		h.historyHealthSignal.AverageLatency(), h.config.HealthRPCLatencyFailure(),
		"historyservice latency"))

	checks = append(checks, errorIfOverThreshold(healthcheck.CheckTypeRPCErrorRatio,
		h.historyHealthSignal.ErrorRatio(), h.config.HealthRPCErrorRatio(),
		"historyservice error ratio"))

	checks = append(checks, errorIfOverThreshold(healthcheck.CheckTypePersistenceLatency,
		h.persistenceHealthSignal.AverageLatency(), h.config.HealthPersistenceLatencyFailure(),
		"persistenceservice latency"))

	checks = append(checks, errorIfOverThreshold(healthcheck.CheckTypePersistenceErrRatio,
		h.persistenceHealthSignal.ErrorRatio(), h.config.HealthPersistenceErrorRatio(),
		"persistenceservice error ratio"))

	overallState := enumsspb.HEALTH_STATE_SERVING

	for _, check := range checks {
		if check.State == enumsspb.HEALTH_STATE_NOT_SERVING {
			overallState = check.State
			break
		}
	}

	metrics.HistoryHostHealthGauge.With(h.metricsHandler).Record(float64(overallState))

	return &historyservice.DeepHealthCheckResponse{
		State:  overallState,
		Checks: checks,
	}, nil
}

func suppressStartupErrors(status grpchealthspb.HealthCheckResponse_ServingStatus,
	dur time.Duration, threshold time.Duration,
) enumsspb.HealthState {
	if dur < threshold {
		return enumsspb.HEALTH_STATE_SERVING
	}
	return toLocalHealthProto(status)
}

func errorIfOverThreshold(checkType string, value float64, threshold float64, message string) *healthspb.HealthCheck {
	state := enumsspb.HEALTH_STATE_SERVING
	if value > threshold {
		state = enumsspb.HEALTH_STATE_NOT_SERVING
	}
	return &healthspb.HealthCheck{
		CheckType: checkType,
		State:     state,
		Value:     value,
		Threshold: threshold,
		Message:   message,
	}
}

func toLocalHealthProto(in grpchealthspb.HealthCheckResponse_ServingStatus) enumsspb.HealthState {
	switch in {
	case grpchealthspb.HealthCheckResponse_SERVING:
		return enumsspb.HEALTH_STATE_SERVING
	case grpchealthspb.HealthCheckResponse_NOT_SERVING:
		return enumsspb.HEALTH_STATE_NOT_SERVING
	default:
		return enumsspb.HEALTH_STATE_UNSPECIFIED
	}
}
