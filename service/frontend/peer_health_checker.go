package frontend

import (
	"context"
	"math"
	"math/rand/v2"
	"slices"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

const peerHealthCheckMaxConcurrentProbes = 50

var peerHealthCheckRoles = []primitives.ServiceName{primitives.HistoryService, primitives.MatchingService}

type (
	// peerProbeFn probes one host of the given role and returns an error if it is unhealthy.
	peerProbeFn func(ctx context.Context, role primitives.ServiceName, address string) error

	peerHealthCheckConfig struct {
		Enabled          dynamicconfig.BoolPropertyFn
		Interval         dynamicconfig.DurationPropertyFn
		ProbeTimeout     dynamicconfig.DurationPropertyFn
		RoundTimeout     dynamicconfig.DurationPropertyFn
		SamplePercentage dynamicconfig.FloatPropertyFn
		FailureThreshold dynamicconfig.FloatPropertyFn
		MaxHostsPerRole  dynamicconfig.IntPropertyFn
	}

	peerHealthChecker struct {
		monitor        membership.Monitor
		probe          peerProbeFn
		setStatus      func(healthpb.HealthCheckResponse_ServingStatus)
		config         peerHealthCheckConfig
		metricsHandler metrics.Handler
		logger         log.Logger
	}

	peerProbeResult struct {
		role primitives.ServiceName
		err  error
	}
)

func newPeerHealthChecker(
	monitor membership.Monitor,
	probe peerProbeFn,
	setStatus func(healthpb.HealthCheckResponse_ServingStatus),
	config peerHealthCheckConfig,
	metricsHandler metrics.Handler,
	logger log.Logger,
) *peerHealthChecker {
	return &peerHealthChecker{
		monitor:        monitor,
		probe:          probe,
		setStatus:      setStatus,
		config:         config,
		metricsHandler: metricsHandler,
		logger:         logger,
	}
}

// samplePeerHosts returns a random subset of hosts without mutating the input.
// The size is min(len(hosts), maxHosts, max(1, ceil(len(hosts)*pct))), or 0 when hosts is empty.
func samplePeerHosts(hosts []membership.HostInfo, pct float64, maxHosts int) []membership.HostInfo {
	if len(hosts) == 0 {
		return nil
	}
	n := min(len(hosts), maxHosts, max(1, int(math.Ceil(float64(len(hosts))*pct))))
	shuffled := slices.Clone(hosts)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:n]
}

// runRound probes a random sample of hosts for each role and reports whether the round passed.
// A round fails if any role has no sampled hosts, its failure ratio reaches the threshold, or the round deadline is exceeded.
func (c *peerHealthChecker) runRound(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, c.config.RoundTimeout())
	defer cancel()

	type probeTarget struct {
		role    primitives.ServiceName
		address string
	}
	var targets []probeTarget
	sampledPerRole := make(map[primitives.ServiceName]int, len(peerHealthCheckRoles))
	for _, role := range peerHealthCheckRoles {
		resolver, err := c.monitor.GetResolver(role)
		if err != nil {
			c.logger.Warn("peer health check failed to get resolver", tag.Service(role), tag.Error(err))
			return false
		}
		hosts := samplePeerHosts(resolver.AvailableMembers(), c.config.SamplePercentage(), c.config.MaxHostsPerRole())
		if len(hosts) == 0 {
			c.logger.Warn("peer health check found no available hosts", tag.Service(role))
			return false
		}
		sampledPerRole[role] = len(hosts)
		for _, h := range hosts {
			targets = append(targets, probeTarget{role: role, address: h.GetAddress()})
		}
	}

	// Buffered to the total probe count so probes that outlive the round never block.
	results := make(chan peerProbeResult, len(targets))
	sem := make(chan struct{}, peerHealthCheckMaxConcurrentProbes)
	for _, t := range targets {
		go func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			probeCtx, probeCancel := context.WithTimeout(ctx, c.config.ProbeTimeout())
			defer probeCancel()
			err := c.probe(probeCtx, t.role, t.address)
			if err != nil {
				c.logger.Debug("peer health probe failed", tag.Service(t.role), tag.NewStringTag("address", t.address), tag.Error(err))
			}
			results <- peerProbeResult{role: t.role, err: err}
		}()
	}

	failuresPerRole := make(map[primitives.ServiceName]int, len(peerHealthCheckRoles))
	for range targets {
		select {
		case r := <-results:
			if r.err != nil {
				failuresPerRole[r.role]++
			}
		case <-ctx.Done():
			c.logger.Warn("peer health check round timed out", tag.Error(ctx.Err()))
			return false
		}
	}

	threshold := c.config.FailureThreshold()
	passed := true
	for _, role := range peerHealthCheckRoles {
		if float64(failuresPerRole[role])/float64(sampledPerRole[role]) >= threshold {
			passed = false
		}
	}
	if !passed {
		for _, role := range peerHealthCheckRoles {
			c.logger.Warn("peer health check role failed",
				tag.Service(role),
				tag.NewInt("sampled", sampledPerRole[role]),
				tag.NewInt("failed", failuresPerRole[role]))
		}
	}
	return passed
}
