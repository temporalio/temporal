package frontend

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"time"

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
		Check(ctx context.Context, now time.Time) (HealthCheckResult, error)
	}

	healthCheckerImpl struct {
		serviceName                   primitives.ServiceName
		membershipMonitor             membership.Monitor
		hostFailurePercentage         dynamicconfig.FloatPropertyFn
		hostDeclinedServingProportion dynamicconfig.FloatPropertyFn
		hostFailureTimeThreshold      dynamicconfig.DurationPropertyFn
		healthCheckFn                 func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error)
		recentUnhealthyHosts          *unhealthyHostTracker
		logger                        log.Logger
	}

	hostResult struct {
		address  string
		response *historyservice.DeepHealthCheckResponse
	}
	unhealthyHostTracker struct {
		mu    sync.Mutex
		hosts map[string]unhealthyHostRecord
	}
	unhealthyHostRecord struct {
		address        string
		lastSeenHealth enumsspb.HealthState
		failedAt       time.Time
	}
)

// Compile-time assert: healthCheckerImpl implements HealthChecker
var _ HealthChecker = (*healthCheckerImpl)(nil)

// trackUnhealthyHost tracks the last time a host failed health check.
// Must be called with mu held.
func (u *unhealthyHostTracker) trackUnhealthyHost(address string, now time.Time, state enumsspb.HealthState) {
	if existing, present := u.hosts[address]; !present || existing.lastSeenHealth != state {
		// Preserve the earliest time that the host failed, even if it changes states
		var failedAt time.Time
		if present {
			failedAt = slices.MinFunc([]time.Time{now, existing.failedAt}, time.Time.Compare)
		} else {
			failedAt = now
		}
		u.hosts[address] = unhealthyHostRecord{
			address:        address,
			lastSeenHealth: state,
			failedAt:       failedAt,
		}
	}
}

// clearStaleEntries removes any entries from the tracker that are no longer in the list of hosts.
// Must be called with mu held.
func (u *unhealthyHostTracker) clearStaleEntries(hosts []*healthspb.HostHealthDetail) {
	validAddresses := make(map[string]struct{})
	for _, host := range hosts {
		// Keep any addresses that are failing or declined serving.
		if host.GetState() == enumsspb.HEALTH_STATE_SERVING {
			continue
		}
		validAddresses[host.GetAddress()] = struct{}{}
	}
	for address := range u.hosts {
		if _, present := validAddresses[address]; !present {
			delete(u.hosts, address)
		}
	}
}

// unhealthyHosts returns a list of hosts that have failed health check recently.
// Must be called with mu held.
func (u *unhealthyHostTracker) unhealthyHosts(now time.Time, duration time.Duration) (declinedServing []unhealthyHostRecord, otherUnhealthy []unhealthyHostRecord) {
	for _, record := range u.hosts {
		if now.Sub(record.failedAt) >= duration {
			if record.lastSeenHealth == enumsspb.HEALTH_STATE_DECLINED_SERVING {
				declinedServing = append(declinedServing, record)
			} else {
				otherUnhealthy = append(otherUnhealthy, record)
			}
		}
	}
	return
}

// updateUnhealthyHosts updates the local host health entries while holding mu, then returns the unhealthy hosts.
func (u *unhealthyHostTracker) updateUnhealthyHosts(hostHealths []*healthspb.HostHealthDetail, now time.Time,
	threshold time.Duration) (declinedServing []unhealthyHostRecord, otherUnhealthy []unhealthyHostRecord) {
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, result := range hostHealths {
		// Only track unhealthy hosts, healthy hosts will be cleaned up in clearStaleEntries
		if result.GetState() != enumsspb.HEALTH_STATE_SERVING {
			u.trackUnhealthyHost(result.Address, now, result.State)
		}
	}
	u.clearStaleEntries(hostHealths)
	return u.unhealthyHosts(now, threshold)
}

func NewHealthChecker(
	serviceName primitives.ServiceName,
	membershipMonitor membership.Monitor,
	hostFailurePercentage dynamicconfig.FloatPropertyFn,
	hostDeclinedServingProportion dynamicconfig.FloatPropertyFn,
	hostFailureTimeThreshold dynamicconfig.DurationPropertyFn,
	healthCheckFn func(ctx context.Context, hostAddress string) (*historyservice.DeepHealthCheckResponse, error),
	logger log.Logger,
) HealthChecker {
	return &healthCheckerImpl{
		serviceName:                   serviceName,
		membershipMonitor:             membershipMonitor,
		hostFailurePercentage:         hostFailurePercentage,
		hostDeclinedServingProportion: hostDeclinedServingProportion,
		hostFailureTimeThreshold:      hostFailureTimeThreshold,
		healthCheckFn:                 healthCheckFn,
		recentUnhealthyHosts:          &unhealthyHostTracker{hosts: make(map[string]unhealthyHostRecord)},
		logger:                        logger,
	}
}

func (h *healthCheckerImpl) Check(ctx context.Context, now time.Time) (HealthCheckResult, error) {
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

	hostDetails := h.collectHostHealth(ctx, hosts)
	declinedServing, otherUnhealthy := h.recentUnhealthyHosts.updateUnhealthyHosts(hostDetails, now, h.hostFailureTimeThreshold())

	overallState := enumsspb.HEALTH_STATE_SERVING
	hostDeclinedServingProportion := float64(len(declinedServing)) / float64(len(hosts))
	failedHostCountProportion := float64(len(otherUnhealthy)) / float64(len(hosts))

	if failedHostCountProportion+hostDeclinedServingProportion > h.hostFailurePercentage() {
		overallState = enumsspb.HEALTH_STATE_NOT_SERVING
	}
	if len(declinedServing) > 2 && hostDeclinedServingProportion > h.hostDeclinedServingProportion() {
		overallState = enumsspb.HEALTH_STATE_DECLINED_SERVING
	}
	if overallState != enumsspb.HEALTH_STATE_SERVING {
		var exampleFailedHost *healthspb.HostHealthDetail
		for _, host := range hostDetails {
			if host.State != enumsspb.HEALTH_STATE_SERVING {
				exampleFailedHost = host
				break
			}
		}
		h.logger.Warn("Health check determined the service is unhealthy!",
			tag.Int("host failure count", len(otherUnhealthy)),
			tag.Float64("host failure percentage threshold", h.hostFailurePercentage()),
			tag.Float64("host failure percentage", failedHostCountProportion),
			tag.Int("host declined serving count", len(declinedServing)),
			tag.Float64("host declined serving percentage", hostDeclinedServingProportion),
			tag.Float64("host declined serving percentage threshold", h.hostDeclinedServingProportion()),
			tag.NewStringTag("example_failed_host", failedHostSummary(exampleFailedHost)),
		)
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

func (h *healthCheckerImpl) collectHostHealth(ctx context.Context, hosts []membership.HostInfo) []*healthspb.HostHealthDetail {
	hostDetails := make([]*healthspb.HostHealthDetail, 0, len(hosts))
	receiveCh := make(chan hostResult, len(hosts))
	defer close(receiveCh)
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

	for range hosts {
		result := <-receiveCh
		state := result.response.GetState()

		detail := &healthspb.HostHealthDetail{
			Address: result.address,
			State:   state,
			Checks:  result.response.GetChecks(),
		}
		hostDetails = append(hostDetails, detail)
	}
	return hostDetails
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
