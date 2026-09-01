package frontend

import (
	"context"
	"fmt"

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
		historyServiceName    primitives.ServiceName
		membershipMonitor     membership.Monitor
		hostFailurePercentage dynamicconfig.FloatPropertyFn
		healthCheckFn         func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error)
		logger                log.Logger
	}

	hostResult struct {
		address  string
		response *historyservice.DeepHealthCheckResponse
	}
)

func NewHealthChecker(
	historyServiceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error),
	logger log.Logger,
) HealthChecker {
	return &healthCheckerImpl{
		historyServiceName:    historyServiceName,
		membershipMonitor:     membershipMonitor,
		hostFailurePercentage: hostFailurePercentage,
		healthCheckFn:         healthCheckFn,
		logger:                logger,
	}
}

func (h *healthCheckerImpl) Check(ctx context.Context) (HealthCheckResult, error) {
	resolver, err := h.membershipMonitor.GetResolver(h.historyServiceName)
	if err != nil {
		return HealthCheckResult{
			State: enumsspb.HEALTH_STATE_INTERNAL_ERROR,
			ServiceDetail: &healthspb.ServiceHealthDetail{
				Service: string(h.historyServiceName),
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
				Service: string(h.historyServiceName),
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

	var hostDetails []*healthspb.HostHealthDetail

	for range hosts {
		result := <-receiveCh

		detail := &healthspb.HostHealthDetail{
			Address:         result.address,
			State:           result.response.GetState(),
			Checks:          result.response.GetChecks(),
			UnenforcedState: result.response.GetUnenforcedState(),
		}

		hostDetails = append(hostDetails, detail)
	}

	close(receiveCh)

	overallState, unenforcedState := calculateStateAndUnenforcedState(hostDetails, h.hostFailurePercentage())

	return HealthCheckResult{
		State: overallState,
		ServiceDetail: &healthspb.ServiceHealthDetail{
			Service:         string(h.historyServiceName),
			State:           overallState,
			Hosts:           hostDetails,
			UnenforcedState: unenforcedState,
		},
	}, nil
}

func (h *healthCheckerImpl) checkHost(ctx context.Context, hostAddress string) (resp *historyservice.DeepHealthCheckResponse, retErr error) {
	defer log.CapturePanic(h.logger, &retErr)

	resp, err := h.healthCheckFn(ctx, hostAddress)
	if err != nil {
		h.logger.Warn("failed to ping deep health check", tag.Error(err), tag.ServerName(string(h.historyServiceName)))
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

// returns the overall state and the unenforced state
func calculateStateAndUnenforcedState(hostDetails []*healthspb.HostHealthDetail, failureProportionThreshold float64) (enumsspb.HealthState, enumsspb.HealthState) {
	var hostsNotHealthy float64
	var hostsNotHealthyUnenforced float64

	for _, hostDetail := range hostDetails {
		if hostDetail.State != enumsspb.HEALTH_STATE_SERVING {
			hostsNotHealthy++
			hostsNotHealthyUnenforced++
			continue
		}

		if hostDetail.UnenforcedState != enumsspb.HEALTH_STATE_SERVING {
			hostsNotHealthyUnenforced++
		}
	}

	overallState := enumsspb.HEALTH_STATE_SERVING
	unenforcedState := enumsspb.HEALTH_STATE_SERVING

	hostCount := float64(len(hostDetails))

	if hostsNotHealthy/hostCount >= failureProportionThreshold {
		overallState = enumsspb.HEALTH_STATE_NOT_SERVING
		unenforcedState = enumsspb.HEALTH_STATE_NOT_SERVING
	}

	if hostsNotHealthyUnenforced/hostCount >= failureProportionThreshold {
		unenforcedState = enumsspb.HEALTH_STATE_NOT_SERVING
	}

	return overallState, unenforcedState
}
