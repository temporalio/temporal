package frontend

import (
	"context"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
)

type (
	HealthChecker interface {
		Check(ctx context.Context) (enumsspb.HealthState, error)
	}

	healthCheckerImpl struct {
		serviceName           primitives.ServiceName
		membershipMonitor     membership.Monitor
		hostFailurePercentage dynamicconfig.FloatPropertyFn
		healthCheckFn         func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error)
		logger                log.Logger
	}
)

func NewHealthChecker(
	serviceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error),
	logger log.Logger,
) HealthChecker {
	return &healthCheckerImpl{
		serviceName:           serviceName,
		membershipMonitor:     membershipMonitor,
		hostFailurePercentage: hostFailurePercentage,
		healthCheckFn:         healthCheckFn,
		logger:                logger,
	}
}

func (h *healthCheckerImpl) Check(ctx context.Context) (enumsspb.HealthState, error) {
	resolver, err := h.membershipMonitor.GetResolver(h.serviceName)
	if err != nil {
		return enumsspb.HEALTH_STATE_UNSPECIFIED, err
	}

	hosts := resolver.AvailableMembers()
	if len(hosts) == 0 {
		return enumsspb.HEALTH_STATE_SERVING, nil
	}

	receiveCh := make(chan enumsspb.HealthState, len(hosts))
	for _, host := range hosts {
		go func(hostAddress string) {
			resp, err := h.healthCheckFn(
				ctx,
				hostAddress,
			)
			if err != nil {
				h.logger.Warn("failed to ping deep health check", tag.Error(err), tag.ServerName(string(h.serviceName)))
			}
			receiveCh <- resp
		}(host.GetAddress())
	}

	var failedHostCount float64
	for i := 0; i < len(hosts); i++ {
		healthState := <-receiveCh
		if healthState == enumsspb.HEALTH_STATE_NOT_SERVING {
			failedHostCount++
		}
	}
	close(receiveCh)

	if (failedHostCount / float64(len(hosts))) > h.hostFailurePercentage() {
		return enumsspb.HEALTH_STATE_NOT_SERVING, nil
	}

	return enumsspb.HEALTH_STATE_SERVING, nil
}
