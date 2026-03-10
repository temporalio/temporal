package frontend

import (
	"context"
	"fmt"
	"math"

	enumsspb "go.temporal.io/server/api/enums/v1"
	healthspb "go.temporal.io/server/api/health/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/health"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type (
	HealthCheckResult struct {
		State         enumsspb.HealthState
		ServiceDetail *healthspb.ServiceHealthDetail
	}

	HealthChecker interface {
		Check(ctx context.Context) (HealthCheckResult, error)
	}

	healthCheckerImpl struct {
		serviceName                   primitives.ServiceName
		membershipMonitor             membership.Monitor
		hostFailurePercentage         dynamicconfig.FloatPropertyFn
		hostDeclinedServingProportion dynamicconfig.FloatPropertyFn
		healthCheckFn                 func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error)
		logger                        log.Logger
	}

	hostResult struct {
		address  string
		response *historyservice.DeepHealthCheckResponse
	}
)

func NewHealthChecker(
	serviceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	hostDeclinedServingProportion dynamicconfig.FloatPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error),
	logger log.Logger,
) HealthChecker {
	return &healthCheckerImpl{
		serviceName:                   serviceName,
		membershipMonitor:             membershipMonitor,
		hostFailurePercentage:         hostFailurePercentage,
		hostDeclinedServingProportion: hostDeclinedServingProportion,
		healthCheckFn:                 healthCheckFn,
		logger:                        logger,
	}
}

func (h *healthCheckerImpl) Check(ctx context.Context) (HealthCheckResult, error) {
	resolver, err := h.membershipMonitor.GetResolver(h.serviceName)
	if err != nil {
		return HealthCheckResult{
			State: enumsspb.HEALTH_STATE_INTERNAL_ERROR,
			ServiceDetail: &healthspb.ServiceHealthDetail{
				Service: string(h.serviceName),
				State:   enumsspb.HEALTH_STATE_INTERNAL_ERROR,
				Message: fmt.Sprintf("failed to get membership resolver: %v", err),
			},
		}, err
	}

	hosts := resolver.AvailableMembers()
	if len(hosts) == 0 {
		return HealthCheckResult{
			State: enumsspb.HEALTH_STATE_NOT_SERVING,
			ServiceDetail: &healthspb.ServiceHealthDetail{
				Service: string(h.serviceName),
				State:   enumsspb.HEALTH_STATE_NOT_SERVING,
				Message: "no available hosts in membership",
			},
		}, nil
	}

	receiveCh := make(chan hostResult, len(hosts))
	for _, host := range hosts {
		go func(hostAddress string) {
			resp, err := h.checkHost(ctx, hostAddress)
			if err != nil {
				resp = &historyservice.DeepHealthCheckResponse{
					State: enumsspb.HEALTH_STATE_NOT_SERVING,
					Checks: []*healthspb.HealthCheck{
						{
							CheckType: health.CheckTypeHostAvailability,
							State:     enumsspb.HEALTH_STATE_NOT_SERVING,
							Message:   fmt.Sprintf("failed to reach host for health check: %v", err),
						},
					},
				}
			}
			receiveCh <- hostResult{address: hostAddress, response: resp}
		}(host.GetAddress())
	}

	var failedHostCount float64
	var hostDeclinedServingCount float64
	var hostDetails []*healthspb.HostHealthDetail
	var exampleFailedHost *healthspb.HostHealthDetail
	for range hosts {
		result := <-receiveCh
		state := result.response.GetState()

		detail := &healthspb.HostHealthDetail{
			Address: result.address,
			State:   state,
			Checks:  result.response.GetChecks(),
		}
		hostDetails = append(hostDetails, detail)

		switch state {
		case enumsspb.HEALTH_STATE_SERVING:
			// Do nothing.
		case enumsspb.HEALTH_STATE_DECLINED_SERVING:
			hostDeclinedServingCount++
		default:
			// NOT_SERVING, UNSPECIFIED, INTERNAL_ERROR, or any unknown state.
			failedHostCount++
			if exampleFailedHost == nil {
				exampleFailedHost = detail
			}
		}
	}
	close(receiveCh)

	// Make sure that at lease 2 hosts must be not ready to trigger this check.
	proportionOfDeclinedServiceHosts := ensureMinimumProportionOfHosts(h.hostDeclinedServingProportion(), len(hosts))

	var overallState enumsspb.HealthState
	hostDeclinedServingProportion := hostDeclinedServingCount / float64(len(hosts))
	if hostDeclinedServingProportion > proportionOfDeclinedServiceHosts {
		h.logger.Warn("health check exceeded host declined serving proportion threshold", tag.Float64("host declined serving proportion threshold", proportionOfDeclinedServiceHosts))
		overallState = enumsspb.HEALTH_STATE_DECLINED_SERVING
	} else {
		failedHostCountProportion := failedHostCount / float64(len(hosts))
		if failedHostCountProportion+hostDeclinedServingProportion > h.hostFailurePercentage() {
			h.logger.Warn("health check exceeded host failure percentage threshold",
				tag.Float64("host failure percentage threshold", h.hostFailurePercentage()),
				tag.Float64("host failure percentage", failedHostCountProportion),
				tag.Float64("host declined serving percentage", hostDeclinedServingProportion),
				tag.NewStringTag("example_failed_host", failedHostSummary(exampleFailedHost)),
			)
			overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		} else {
			overallState = enumsspb.HEALTH_STATE_SERVING
		}
	}

	return HealthCheckResult{
		State: overallState,
		ServiceDetail: &healthspb.ServiceHealthDetail{
			Service: string(h.serviceName),
			State:   overallState,
			Hosts:   hostDetails,
		},
	}, nil
}

func (h *healthCheckerImpl) checkHost(ctx context.Context, hostAddress string) (resp *historyservice.DeepHealthCheckResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	resp, err := h.healthCheckFn(ctx, hostAddress)
	if err != nil {
		h.logger.Warn("failed to ping deep health check", tag.Error(err), tag.ServerName(string(h.serviceName)))
		return nil, err
	}
	if resp == nil {
		resp = &historyservice.DeepHealthCheckResponse{
			State: enumsspb.HEALTH_STATE_NOT_SERVING,
			Checks: []*healthspb.HealthCheck{
				{
					CheckType: health.CheckTypeHostAvailability,
					State:     enumsspb.HEALTH_STATE_NOT_SERVING,
					Message:   "no response received from health check",
				},
			},
		}
	}
	return resp, nil
}

func failedHostSummary(host *healthspb.HostHealthDetail) string {
	if host == nil {
		return "unknown"
	}
	for _, check := range host.GetChecks() {
		if check.GetState() != enumsspb.HEALTH_STATE_SERVING && check.GetMessage() != "" {
			return fmt.Sprintf("%s: %s", host.GetAddress(), check.GetMessage())
		}
	}
	return fmt.Sprintf("%s: %s", host.GetAddress(), host.GetState().String())
}

func ensureMinimumProportionOfHosts(proportionOfDeclinedServingHosts float64, totalHosts int) float64 {
	const minimumHostsFailed = 2.0 // We want to ensure that at least 2 fail before we notify the upstream.
	minimumProportion := minimumHostsFailed / float64(totalHosts)
	return math.Max(proportionOfDeclinedServingHosts, minimumProportion)
}
