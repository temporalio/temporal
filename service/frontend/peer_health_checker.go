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

// minPeerHealthCheckInterval guards against a non-positive dynamic config value, which would panic time.NewTicker or spin time.NewTimer.
const minPeerHealthCheckInterval = time.Second

// peerProbeOutcome classifies one probe. Timeouts and routing errors count as failures.
type peerProbeOutcome string

const (
	peerProbeResponse   peerProbeOutcome = "response"   // the host answered, with any status
	peerProbeRefused    peerProbeOutcome = "refused"    // connection refused: the host is reachable but not listening
	peerProbeTimeout    peerProbeOutcome = "timeout"    // no answer in time
	peerProbeNetwork    peerProbeOutcome = "network"    // routing error
	peerProbeUnresolved peerProbeOutcome = "unresolved" // DNS error
	peerProbeUnknown    peerProbeOutcome = "unknown"    // canceled or unrecognized
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
	// It must return promptly when its context ends to free a shared probe slot.
	peerProbeFn func(ctx context.Context, role primitives.ServiceName, address string) error

	peerHealthCheckConfig struct {
		Enabled  dynamicconfig.BoolPropertyFn
		Settings dynamicconfig.TypedPropertyFn[dynamicconfig.PeerHealthCheckSettings]
	}

	// peerHealthChecker detects a frontend that cannot reach History or Matching hosts that its own membership view still lists as available.
	// Limits:
	//   - It only probes hosts in the local AvailableMembers view. If membership has already dropped the unreachable hosts,
	//     for example in a partition where frontends on the same side confirm the far side is down, it sees nothing wrong.
	//   - When no sampled host of a role answers, it assumes that role is down for everyone and does not fail,
	//     so a frontend cut off from every host of a role is not detected either.
	peerHealthChecker struct {
		monitor        membership.Monitor
		probe          peerProbeFn
		setStatus      func(healthpb.HealthCheckResponse_ServingStatus)
		config         peerHealthCheckConfig
		metricsHandler metrics.Handler
		logger         log.Logger

		lifecycleMu sync.Mutex // serializes Start and Stop so cancel is set before Stop can read it
		status      atomic.Int32
		cancel      context.CancelFunc
		wg          sync.WaitGroup
		probeSlots  chan struct{}
	}

	peerProbeTarget struct {
		role    primitives.ServiceName
		address string
	}

	peerProbeResult struct {
		role    primitives.ServiceName
		outcome peerProbeOutcome
		err     error
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
		probeSlots:     make(chan struct{}, peerHealthCheckMaxConcurrentProbes),
	}
}

// samplePeerHosts returns a random subset of hosts without mutating the input.
// When possible, the sample includes room for the minimum number of failed probes and a reachable host.
func samplePeerHosts(hosts []membership.HostInfo, pct float64, minFailedHosts int) []membership.HostInfo {
	if len(hosts) == 0 {
		return nil
	}
	minSample := min(len(hosts)-1, minFailedHosts) + 1
	n := min(len(hosts), max(minSample, int(math.Ceil(float64(len(hosts))*pct))))
	shuffled := slices.Clone(hosts)
	rand.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:n]
}

// runRound probes a random sample of hosts for each role and returns the round verdict: pass, fail or unknown.
// A role without members or a resolver makes the verdict unknown, unless another role fails.
func (c *peerHealthChecker) runRound(ctx context.Context) string {
	if ctx.Err() != nil {
		return peerRoundUnknown
	}
	minFailedHosts := c.minFailedHosts()
	targets, plannedPerRole := c.sampleProbeTargets(minFailedHosts)
	if len(targets) == 0 {
		return peerRoundUnknown
	}
	results := make(chan peerProbeResult)
	go c.launchProbes(ctx, targets, results)
	counts, representativeErrors := c.collectProbeResults(ctx, plannedPerRole, results)
	if ctx.Err() != nil {
		return peerRoundUnknown
	}
	return c.evaluateRound(plannedPerRole, counts, representativeErrors, minFailedHosts)
}

// sampleProbeTargets samples hosts for every role. A role with a resolver error or no available hosts is logged and left out.
func (c *peerHealthChecker) sampleProbeTargets(minFailedHosts int) ([]peerProbeTarget, map[primitives.ServiceName]int) {
	var targets []peerProbeTarget
	plannedPerRole := make(map[primitives.ServiceName]int, len(peerHealthCheckRoles))
	percentage := c.samplePercentage()
	for _, role := range peerHealthCheckRoles {
		resolver, err := c.monitor.GetResolver(role)
		if err != nil {
			c.logger.Warn("peer health check failed to get resolver", tag.NewStringTag("target-service", string(role)), tag.Error(err))
			continue
		}
		hosts := samplePeerHosts(resolver.AvailableMembers(), percentage, minFailedHosts)
		if len(hosts) == 0 {
			c.logger.Warn("peer health check found no available hosts", tag.NewStringTag("target-service", string(role)))
			continue
		}
		plannedPerRole[role] = len(hosts)
		for _, h := range hosts {
			targets = append(targets, peerProbeTarget{role: role, address: h.GetAddress()})
		}
	}
	return targets, plannedPerRole
}

// launchProbes acquires a shared slot before starting each probe goroutine.
func (c *peerHealthChecker) launchProbes(ctx context.Context, targets []peerProbeTarget, results chan<- peerProbeResult) {
	for _, target := range targets {
		if ctx.Err() != nil {
			return
		}
		select {
		case c.probeSlots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		go func() {
			defer func() { <-c.probeSlots }()
			if ctx.Err() != nil {
				return
			}
			result := c.runProbe(ctx, target)
			select {
			case results <- result:
			case <-ctx.Done():
			}
		}()
	}
}

func (c *peerHealthChecker) runProbe(ctx context.Context, target peerProbeTarget) peerProbeResult {
	probeCtx, cancel := context.WithTimeout(ctx, c.probeTimeout())
	defer cancel()
	err := c.probe(probeCtx, target.role, target.address)
	outcome := classifyPeerProbeError(ctx, probeCtx, err)
	if err != nil {
		c.logger.Debug("peer health probe failed", tag.NewStringTag("target-service", string(target.role)), tag.NewStringTag("address", target.address), tag.NewStringTag("outcome", string(outcome)), tag.Error(err))
	}
	return peerProbeResult{role: target.role, outcome: outcome, err: err}
}

// classifyPeerProbeError distinguishes checker cancellation from an individual probe timeout.
func classifyPeerProbeError(ctx, probeCtx context.Context, err error) peerProbeOutcome {
	if ctx.Err() != nil {
		return peerProbeUnknown
	}
	if err == nil {
		return peerProbeResponse
	}
	// Internode connections convert RPC errors to serviceerror types, which status.Code does not understand.
	code := serviceerror.ToStatus(err).Code()
	_, isGRPC := status.FromError(err)
	_, isServiceError := errors.AsType[serviceerror.ServiceError](err)

	if (isGRPC || isServiceError) && code != codes.Unavailable && code != codes.DeadlineExceeded && code != codes.Canceled {
		// A non-transport application status confirms that the peer answered, even if the probe deadline expires just afterward.
		return peerProbeResponse
	}
	if code == codes.Canceled || errors.Is(err, context.Canceled) {
		// Closing the local gRPC connection can produce Canceled while the probe context is live.
		return peerProbeUnknown
	}
	if probeCtx.Err() != nil {
		if errors.Is(probeCtx.Err(), context.DeadlineExceeded) {
			return peerProbeTimeout
		}
		return peerProbeUnknown
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return peerProbeTimeout
	case code == codes.DeadlineExceeded:
		// grpc-go can observe an elapsed deadline before the context timer signals cancellation.
		if deadline, ok := probeCtx.Deadline(); ok && time.Until(deadline) <= 0 {
			return peerProbeTimeout
		}
		return peerProbeResponse
	}
	return classifyPeerTransportError(err.Error(), code == codes.Unavailable && (isGRPC || isServiceError))
}

func classifyPeerTransportError(msg string, unavailableStatus bool) peerProbeOutcome {
	if unavailableStatus {
		hasTransportDetail := strings.Contains(msg, "transport: Error while dialing") || strings.Contains(msg, "connection error: desc") || strings.Contains(msg, "closing transport due to:") || strings.Contains(msg, "dial tcp ") || strings.Contains(msg, "dial: ") || strings.Contains(msg, "lookup ")
		if !hasTransportDetail {
			// The internode client also wraps peer application errors as Unavailable.
			return peerProbeUnknown
		}
	}
	// These strings come from grpc-go transport errors or direct dial failures.
	switch {
	case strings.Contains(msg, "connection refused"):
		return peerProbeRefused
	case strings.Contains(msg, "i/o timeout"), strings.Contains(msg, "context deadline exceeded"), strings.Contains(msg, "connection timed out"), strings.Contains(msg, "operation timed out"):
		return peerProbeTimeout
	case strings.Contains(msg, "no such host"):
		return peerProbeUnresolved
	case strings.Contains(msg, "no route to host"), strings.Contains(msg, "network is unreachable"):
		if strings.Contains(msg, "lookup ") {
			// The routing error may be for the DNS server.
			return peerProbeUnresolved
		}
		return peerProbeNetwork
	}
	return peerProbeUnknown
}

// collectProbeResults tallies probe outcomes per role and records metrics once all probes report.
// Cancellation discards the round.
func (c *peerHealthChecker) collectProbeResults(
	ctx context.Context,
	plannedPerRole map[primitives.ServiceName]int,
	results <-chan peerProbeResult,
) (map[primitives.ServiceName]map[peerProbeOutcome]int, map[primitives.ServiceName]error) {
	counts := make(map[primitives.ServiceName]map[peerProbeOutcome]int, len(plannedPerRole))
	representativeErrors := make(map[primitives.ServiceName]error, len(plannedPerRole))
	total := 0
	for role, planned := range plannedPerRole {
		counts[role] = make(map[peerProbeOutcome]int)
		total += planned
	}
	for range total {
		select {
		case r := <-results:
			counts[r.role][r.outcome]++
			if (r.outcome == peerProbeTimeout || r.outcome == peerProbeNetwork) && representativeErrors[r.role] == nil {
				representativeErrors[r.role] = r.err
			}
		case <-ctx.Done():
			return nil, nil
		}
	}
	if ctx.Err() != nil {
		return nil, nil
	}
	for role, outcomes := range counts {
		for outcome, n := range outcomes {
			c.metricsHandler.Counter(metrics.FrontendPeerHealthCheckProbe.Name()).
				Record(int64(n), metrics.StringTag("target_service", string(role)), metrics.OutcomeTag(string(outcome)))
		}
	}
	return counts, representativeErrors
}

// evaluateRound judges each role on its own, so roles are never combined.
// A role fails when timeouts and routing errors meet both the ratio and minimum count thresholds.
// A role where no sampled host responded or refused is a downstream outage, not a frontend problem, so it does not fail.
func (c *peerHealthChecker) evaluateRound(
	plannedPerRole map[primitives.ServiceName]int,
	counts map[primitives.ServiceName]map[peerProbeOutcome]int,
	representativeErrors map[primitives.ServiceName]error,
	minFailedHosts int,
) string {
	threshold := c.failureThreshold()
	verdict := peerRoundPass
	for _, role := range peerHealthCheckRoles {
		planned := plannedPerRole[role]
		if planned == 0 {
			// No evidence for this role, so the round cannot report SERVING. A failure in another role still wins.
			if verdict == peerRoundPass {
				verdict = peerRoundUnknown
			}
			continue
		}
		roleCounts := counts[role]
		failures := roleCounts[peerProbeTimeout] + roleCounts[peerProbeNetwork]
		logRole := func(msg string) {
			tags := []tag.Tag{
				tag.NewStringTag("target-service", string(role)),
				tag.NewInt("planned", planned),
				tag.NewInt("timeout", roleCounts[peerProbeTimeout]),
				tag.NewInt("network", roleCounts[peerProbeNetwork]),
				tag.NewInt("refused", roleCounts[peerProbeRefused]),
				tag.NewInt("unresolved", roleCounts[peerProbeUnresolved]),
				tag.NewInt("unknown", roleCounts[peerProbeUnknown]),
			}
			if err := representativeErrors[role]; err != nil {
				tags = append(tags, tag.Error(err))
			}
			c.logger.Warn(msg, tags...)
		}
		if roleCounts[peerProbeResponse]+roleCounts[peerProbeRefused] == 0 {
			if roleCounts[peerProbeUnknown] > 0 {
				// Uncertain probe evidence cannot support a downstream-outage pass.
				logRole("peer health check has unknown probe outcomes and no reachable hosts")
				if verdict == peerRoundPass {
					verdict = peerRoundUnknown
				}
				continue
			}
			logRole("peer health check found no reachable hosts, treating as downstream outage")
			continue
		}
		meetsFailureThreshold := func(n int) bool {
			return n >= minFailedHosts && float64(n)/float64(planned) >= threshold
		}
		if meetsFailureThreshold(failures) {
			logRole("peer health check role failed")
			verdict = peerRoundFail
		} else if verdict == peerRoundPass && meetsFailureThreshold(failures+roleCounts[peerProbeUnknown]) {
			// The inconclusive probes could change the verdict.
			verdict = peerRoundUnknown
		}
	}
	return verdict
}

// Start launches the check loop. It is idempotent.
func (c *peerHealthChecker) Start() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if !c.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go c.loop(ctx)
}

// Stop cancels the loop and waits for it. It is idempotent.
// Stop before Start prevents a later Start. After Stop returns, setStatus is never called again.
func (c *peerHealthChecker) Stop() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if c.status.CompareAndSwap(common.DaemonStatusInitialized, common.DaemonStatusStopped) {
		return
	}
	if !c.status.CompareAndSwap(common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	c.cancel()
	c.wg.Wait()
}

// interval returns the live check interval, floored so a bad dynamic config value cannot crash or spin the loop.
func (c *peerHealthChecker) interval() time.Duration {
	if d := c.config.Settings().Interval; d > 0 {
		return d
	}
	return minPeerHealthCheckInterval
}

// The accessors below fall back to defaults for out-of-range dynamic config values, so a bad value cannot fail every frontend closed at once.

func (c *peerHealthChecker) probeTimeout() time.Duration {
	if d := c.config.Settings().ProbeTimeout; d > 0 {
		return d
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.ProbeTimeout
}

func (c *peerHealthChecker) samplePercentage() float64 {
	if p := c.config.Settings().SamplePercentage; p > 0 && p <= 1 {
		return p
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.SamplePercentage
}

func (c *peerHealthChecker) failureThreshold() float64 {
	if t := c.config.Settings().FailureThreshold; t > 0 && t <= 1 {
		return t
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.FailureThreshold
}

func (c *peerHealthChecker) minFailedHosts() int {
	if n := c.config.Settings().MinFailedHosts; n >= 1 {
		return n
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.MinFailedHosts
}

func (c *peerHealthChecker) loop(ctx context.Context) {
	defer c.wg.Done()
	for {
		c.tick(ctx)
		timer := time.NewTimer(c.interval())
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
