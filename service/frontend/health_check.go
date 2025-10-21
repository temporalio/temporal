package frontend

import (
	"context"
	"math"

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
		serviceName                   primitives.ServiceName
		membershipMonitor             membership.Monitor
		hostFailurePercentage         dynamicconfig.FloatPropertyFn
		hostDeclinedServingProportion dynamicconfig.FloatPropertyFn
		healthCheckFn                 func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error)
		logger                        log.Logger
	}
)

func NewHealthChecker(
	serviceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	hostDeclinedServingProportion dynamicconfig.FloatPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (enumsspb.HealthState, error),
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

func (h *healthCheckerImpl) Check(ctx context.Context) (enumsspb.HealthState, error) {
	resolver, err := h.membershipMonitor.GetResolver(h.serviceName)
	if err != nil {
		return enumsspb.HEALTH_STATE_UNSPECIFIED, err
	}

	hosts := resolver.AvailableMembers()
	if len(hosts) == 0 {
		return enumsspb.HEALTH_STATE_NOT_SERVING, nil
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
	var hostDeclinedServingCount float64
	for i := 0; i < len(hosts); i++ {
		healthState := <-receiveCh
		switch healthState {
		case enumsspb.HEALTH_STATE_NOT_SERVING, enumsspb.HEALTH_STATE_UNSPECIFIED:
			failedHostCount++
		case enumsspb.HEALTH_STATE_DECLINED_SERVING:
			hostDeclinedServingCount++
		case enumsspb.HEALTH_STATE_SERVING:
			// Do nothing.
		}
	}
	close(receiveCh)

	// Make sure that at lease 2 hosts must be not ready to trigger this check.
	proportionOfDeclinedServiceHosts := ensureMinimumProportionOfHosts(h.hostDeclinedServingProportion(), len(hosts))

	hostDeclinedServingProportion := hostDeclinedServingCount / float64(len(hosts))
	if hostDeclinedServingProportion > proportionOfDeclinedServiceHosts {
		h.logger.Warn("health check exceeded host declined serving proportion threshold", tag.NewFloat64("host declined serving proportion threshold", proportionOfDeclinedServiceHosts))
		return enumsspb.HEALTH_STATE_DECLINED_SERVING, nil
	}

	failedHostCountProportion := failedHostCount / float64(len(hosts))
	if failedHostCountProportion+hostDeclinedServingProportion > h.hostFailurePercentage() {
		h.logger.Warn("health check exceeded host failure percentage threshold", tag.NewFloat64("host failure percentage threshold", h.hostFailurePercentage()), tag.NewFloat64("host failure percentage", failedHostCountProportion), tag.NewFloat64("host declined serving percentage", hostDeclinedServingProportion))
		return enumsspb.HEALTH_STATE_NOT_SERVING, nil
	}

	return enumsspb.HEALTH_STATE_SERVING, nil
}

func ensureMinimumProportionOfHosts(proportionOfDeclinedServingHosts float64, totalHosts int) float64 {
	const minimumHostsFailed = 2.0 // We want to ensure that at least 2 fail before we notify the upstream.
	minimumProportion := minimumHostsFailed / float64(totalHosts)
	return math.Max(proportionOfDeclinedServingHosts, minimumProportion)
}
