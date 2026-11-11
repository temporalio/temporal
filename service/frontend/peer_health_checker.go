package frontend

import (
	"context"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

const peerHealthCheckMaxConcurrentProbes = 50

// peerProbeOutcome classifies one probe. Only timeouts count as failures.
type peerProbeOutcome string

const (
	peerProbeResponse   peerProbeOutcome = "response"   // the host answered, with any status
	peerProbeRefused    peerProbeOutcome = "refused"    // connection refused: the host is reachable but not listening
	peerProbeTimeout    peerProbeOutcome = "timeout"    // no answer in time
	peerProbeUnresolved peerProbeOutcome = "unresolved" // DNS or routing error
	peerProbeUnknown    peerProbeOutcome = "unknown"    // cancelled, cut off by the round deadline, or unrecognized
)

const (
	peerRoundPass    = "pass"
	peerRoundFail    = "fail"
	peerRoundUnknown = "unknown"
)

var peerHealthCheckRoles = []primitives.ServiceName{primitives.HistoryService, primitives.MatchingService}

type (
	// peerProbeFn probes one host of the given role. It returns nil for any answer from the host, NOT_SERVING included,
	// and otherwise returns the raw RPC error, unwrapped or wrapped so errors.Is and status.Code still work, for the checker to classify.
	peerProbeFn func(ctx context.Context, role primitives.ServiceName, address string) error

	peerHealthCheckConfig struct {
		Enabled          dynamicconfig.BoolPropertyFn
		Interval         dynamicconfig.DurationPropertyFn
		ProbeTimeout     dynamicconfig.DurationPropertyFn
		RoundTimeout     dynamicconfig.DurationPropertyFn
		SamplePercentage dynamicconfig.FloatPropertyFn
		FailureThreshold dynamicconfig.FloatPropertyFn
		MaxHostsPerRole  dynamicconfig.IntPropertyFn
		MinFailedHosts   dynamicconfig.IntPropertyFn
	}

	peerHealthChecker struct {
		monitor        membership.Monitor
		probe          peerProbeFn
		setStatus      func(healthpb.HealthCheckResponse_ServingStatus)
		config         peerHealthCheckConfig
		metricsHandler metrics.Handler
		logger         log.Logger

		status int32
		cancel context.CancelFunc
		wg     sync.WaitGroup
	}

	peerProbeTarget struct {
		role    primitives.ServiceName
		address string
	}

	peerProbeResult struct {
		role    primitives.ServiceName
		outcome peerProbeOutcome
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

// runRound probes a random sample of hosts for each role and returns the round verdict: pass, fail or unknown.
// Probes the round deadline cuts off, or that never start, count as unknown. A role without members or a resolver is left out.
// The verdict is unknown when no role could be probed.
func (c *peerHealthChecker) runRound(ctx context.Context) string {
	ctx, cancel := context.WithTimeout(ctx, c.config.RoundTimeout())
	defer cancel()

	targets, plannedPerRole := c.sampleProbeTargets()
	if len(targets) == 0 {
		return peerRoundUnknown
	}
	// Buffered to the total probe count so probes that outlive the round never block.
	results := make(chan peerProbeResult, len(targets))
	c.launchProbes(ctx, targets, results)
	counts := c.collectProbeResults(ctx, plannedPerRole, results)
	return c.evaluateRound(plannedPerRole, counts)
}

// sampleProbeTargets samples hosts for every role. A role with a resolver error or no available hosts is logged and left out.
func (c *peerHealthChecker) sampleProbeTargets() ([]peerProbeTarget, map[primitives.ServiceName]int) {
	var targets []peerProbeTarget
	plannedPerRole := make(map[primitives.ServiceName]int, len(peerHealthCheckRoles))
	for _, role := range peerHealthCheckRoles {
		resolver, err := c.monitor.GetResolver(role)
		if err != nil {
			c.logger.Warn("peer health check failed to get resolver", tag.Service(role), tag.Error(err))
			continue
		}
		hosts := samplePeerHosts(resolver.AvailableMembers(), c.config.SamplePercentage(), c.config.MaxHostsPerRole())
		if len(hosts) == 0 {
			c.logger.Warn("peer health check found no available hosts", tag.Service(role))
			continue
		}
		plannedPerRole[role] = len(hosts)
		for _, h := range hosts {
			targets = append(targets, peerProbeTarget{role: role, address: h.GetAddress()})
		}
	}
	return targets, plannedPerRole
}

// launchProbes starts one goroutine per target, with at most peerHealthCheckMaxConcurrentProbes running at once.
// A goroutine that is still waiting for a slot when the round ends exits without sending a result.
func (c *peerHealthChecker) launchProbes(ctx context.Context, targets []peerProbeTarget, results chan<- peerProbeResult) {
	sem := make(chan struct{}, peerHealthCheckMaxConcurrentProbes)
	for _, target := range targets {
		go func() {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()
			results <- c.runProbe(ctx, target)
		}()
	}
}

func (c *peerHealthChecker) runProbe(ctx context.Context, target peerProbeTarget) peerProbeResult {
	probeCtx, cancel := context.WithTimeout(ctx, c.config.ProbeTimeout())
	defer cancel()
	err := c.probe(probeCtx, target.role, target.address)
	outcome := classifyPeerProbeError(ctx, err)
	if err != nil {
		c.logger.Debug("peer health probe failed", tag.Service(target.role), tag.NewStringTag("address", target.address), tag.NewStringTag("outcome", string(outcome)), tag.Error(err))
	}
	return peerProbeResult{role: target.role, outcome: outcome}
}

// classifyPeerProbeError maps a probe error to an outcome. roundCtx is the round context, not the probe context.
func classifyPeerProbeError(roundCtx context.Context, err error) peerProbeOutcome {
	if err == nil {
		return peerProbeResponse
	}
	// Internode connections convert RPC errors to serviceerror types, which status.Code does not understand.
	code := serviceerror.ToStatus(err).Code()
	switch {
	case code == codes.DeadlineExceeded || errors.Is(err, context.DeadlineExceeded):
		// The round deadline cutting a probe short says nothing about the host.
		if roundCtx.Err() != nil {
			return peerProbeUnknown
		}
		return peerProbeTimeout
	case code == codes.Canceled || errors.Is(err, context.Canceled):
		return peerProbeUnknown
	}
	var svcErr serviceerror.ServiceError
	if _, isGRPC := status.FromError(err); (isGRPC || errors.As(err, &svcErr)) && code != codes.Unavailable {
		// The server answered, e.g. PermissionDenied or NotFound.
		return peerProbeResponse
	}
	// shortcut: matches grpc-go transport error text; switch to a custom dialer if the text proves unstable.
	msg := err.Error()
	switch {
	case strings.Contains(msg, "connection refused"):
		return peerProbeRefused
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "context deadline exceeded"):
		return peerProbeTimeout
	case strings.Contains(msg, "no such host"), strings.Contains(msg, "no route to host"), strings.Contains(msg, "network is unreachable"):
		return peerProbeUnresolved
	}
	return peerProbeUnknown
}

// collectProbeResults tallies probe outcomes per role until every probe reports or the round ends.
// Every planned probe that has not reported by then counts as unknown, so each role's counts sum to its planned count.
// It records one probe metric per planned probe, unless the round was cut short by Stop.
func (c *peerHealthChecker) collectProbeResults(
	ctx context.Context,
	plannedPerRole map[primitives.ServiceName]int,
	results <-chan peerProbeResult,
) map[primitives.ServiceName]map[peerProbeOutcome]int {
	counts := make(map[primitives.ServiceName]map[peerProbeOutcome]int, len(plannedPerRole))
	total := 0
	for role, planned := range plannedPerRole {
		counts[role] = make(map[peerProbeOutcome]int)
		total += planned
	}
collect:
	for range total {
		select {
		case r := <-results:
			counts[r.role][r.outcome]++
		case <-ctx.Done():
			c.logger.Warn("peer health check round timed out", tag.Error(ctx.Err()))
			break collect
		}
	}
	// A round cut short by Stop is discarded by tick, so it records nothing.
	if errors.Is(ctx.Err(), context.Canceled) {
		return counts
	}
	for role, planned := range plannedPerRole {
		reported := 0
		for _, n := range counts[role] {
			reported += n
		}
		counts[role][peerProbeUnknown] += planned - reported
		for outcome, n := range counts[role] {
			if n > 0 {
				c.metricsHandler.Counter(metrics.FrontendPeerHealthCheckProbe.Name()).
					Record(int64(n), metrics.StringTag("target_service", string(role)), metrics.OutcomeTag(string(outcome)))
			}
		}
	}
	return counts
}

// evaluateRound judges each role on its own, so roles are never combined.
// A role fails when its timed-out ratio reaches the threshold and at least minFailedHosts probes timed out.
// A role where no sampled host responded or refused is a downstream outage, not a frontend problem, so it does not fail.
func (c *peerHealthChecker) evaluateRound(
	plannedPerRole map[primitives.ServiceName]int,
	counts map[primitives.ServiceName]map[peerProbeOutcome]int,
) string {
	threshold := c.config.FailureThreshold()
	minFailedHosts := c.config.MinFailedHosts()
	verdict := peerRoundPass
	for _, role := range peerHealthCheckRoles {
		planned := plannedPerRole[role]
		if planned == 0 {
			continue
		}
		roleCounts := counts[role]
		timeouts := roleCounts[peerProbeTimeout]
		logRole := func(msg string) {
			c.logger.Warn(msg,
				tag.Service(role),
				tag.NewInt("planned", planned),
				tag.NewInt("timeout", timeouts),
				tag.NewInt("refused", roleCounts[peerProbeRefused]),
				tag.NewInt("unknown", roleCounts[peerProbeUnknown]))
		}
		if roleCounts[peerProbeResponse]+roleCounts[peerProbeRefused] == 0 {
			logRole("peer health check found no reachable hosts, treating as downstream outage")
			continue
		}
		if float64(timeouts)/float64(planned) >= threshold && timeouts >= minFailedHosts {
			logRole("peer health check role failed")
			verdict = peerRoundFail
		}
	}
	return verdict
}

// Start launches the check loop. It is idempotent.
func (c *peerHealthChecker) Start() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go c.loop(ctx)
}

// Stop cancels the loop and waits for it. It is idempotent.
// After Stop returns, setStatus is never called again.
func (c *peerHealthChecker) Stop() {
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	c.cancel()
	c.wg.Wait()
}

func (c *peerHealthChecker) loop(ctx context.Context) {
	defer c.wg.Done()
	for {
		c.tick(ctx)
		timer := time.NewTimer(c.config.Interval())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// tick runs one check iteration. A round interrupted by Stop sets no status and records nothing.
// An unknown round leaves the status unchanged.
func (c *peerHealthChecker) tick(ctx context.Context) {
	if !c.config.Enabled() {
		if ctx.Err() == nil {
			c.setStatus(healthpb.HealthCheckResponse_SERVING)
		}
		return
	}
	verdict := c.runRound(ctx)
	if ctx.Err() != nil {
		return
	}
	switch verdict {
	case peerRoundPass:
		c.setStatus(healthpb.HealthCheckResponse_SERVING)
	case peerRoundFail:
		c.setStatus(healthpb.HealthCheckResponse_NOT_SERVING)
	default:
		// An unknown verdict leaves the status unchanged.
	}
	c.metricsHandler.Counter(metrics.FrontendPeerHealthCheckRound.Name()).Record(1, metrics.OutcomeTag(verdict))
}
