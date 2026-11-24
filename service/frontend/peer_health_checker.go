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
		status      int32
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
// The size is min(len(hosts), maxHosts, max(1, ceil(len(hosts)*pct))), or 0 when hosts is empty or maxHosts is not positive.
func samplePeerHosts(hosts []membership.HostInfo, pct float64, maxHosts int) []membership.HostInfo {
	if len(hosts) == 0 || maxHosts <= 0 {
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
// Probes the round deadline cuts off, or that never start, count as unknown. A role without members or a resolver makes the verdict unknown, unless another role fails.
// The verdict is unknown when no role could be probed.
func (c *peerHealthChecker) runRound(ctx context.Context) string {
	parentCtx := ctx
	ctx, cancel := context.WithTimeout(parentCtx, c.roundTimeout())
	defer cancel()

	targets, plannedPerRole := c.sampleProbeTargets()
	if len(targets) == 0 {
		return peerRoundUnknown
	}
	// Buffered to the total probe count so probes that outlive the round never block.
	results := make(chan peerProbeResult, len(targets))
	c.launchProbes(ctx, targets, results)
	counts, representativeErrors := c.collectProbeResults(ctx, plannedPerRole, results)
	if parentCtx.Err() != nil {
		return peerRoundUnknown
	}
	return c.evaluateRound(plannedPerRole, counts, representativeErrors)
}

// sampleProbeTargets samples hosts for every role. A role with a resolver error or no available hosts is logged and left out.
func (c *peerHealthChecker) sampleProbeTargets() ([]peerProbeTarget, map[primitives.ServiceName]int) {
	var targets []peerProbeTarget
	plannedPerRole := make(map[primitives.ServiceName]int, len(peerHealthCheckRoles))
	for _, role := range peerHealthCheckRoles {
		resolver, err := c.monitor.GetResolver(role)
		if err != nil {
			c.logger.Warn("peer health check failed to get resolver", tag.NewStringTag("target-service", string(role)), tag.Error(err))
			continue
		}
		hosts := samplePeerHosts(resolver.AvailableMembers(), c.samplePercentage(), c.maxHostsPerRole())
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

// launchProbes starts one goroutine per target, with at most peerHealthCheckMaxConcurrentProbes running at once across rounds.
// A goroutine that is still waiting for a slot when the round ends exits without sending a result.
func (c *peerHealthChecker) launchProbes(ctx context.Context, targets []peerProbeTarget, results chan<- peerProbeResult) {
	for _, target := range targets {
		go func() {
			select {
			case c.probeSlots <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-c.probeSlots }()
			if ctx.Err() != nil {
				return
			}
			results <- c.runProbe(ctx, target)
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

// classifyPeerProbeError maps a probe error to an outcome using both round and probe context state.
func classifyPeerProbeError(roundCtx, probeCtx context.Context, err error) peerProbeOutcome {
	if roundCtx.Err() != nil {
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
		if deadline, ok := roundCtx.Deadline(); ok && time.Until(deadline) <= 0 {
			return peerProbeUnknown
		}
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
) (map[primitives.ServiceName]map[peerProbeOutcome]int, map[primitives.ServiceName]error) {
	counts := make(map[primitives.ServiceName]map[peerProbeOutcome]int, len(plannedPerRole))
	representativeErrors := make(map[primitives.ServiceName]error, len(plannedPerRole))
	record := func(r peerProbeResult) {
		counts[r.role][r.outcome]++
		if r.outcome == peerProbeTimeout && representativeErrors[r.role] == nil {
			representativeErrors[r.role] = r.err
		}
	}
	total := 0
	for role, planned := range plannedPerRole {
		counts[role] = make(map[peerProbeOutcome]int)
		total += planned
	}
collect:
	for range total {
		select {
		case r := <-results:
			record(r)
		case <-ctx.Done():
			// Results already in the channel are real answers, so count them before giving up on the rest.
			for len(results) > 0 {
				record(<-results)
			}
			// Only a round deadline is worth a warning; a cancelled parent means shutdown.
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				c.logger.Warn("peer health check round timed out", tag.Error(ctx.Err()))
			}
			break collect
		}
	}
	// A round cut short by Stop is discarded by tick, so it records nothing.
	if errors.Is(ctx.Err(), context.Canceled) {
		return counts, representativeErrors
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
	return counts, representativeErrors
}

// evaluateRound judges each role on its own, so roles are never combined.
// A role fails when its timed-out ratio reaches the threshold and at least minFailedHosts probes timed out.
// A role where no sampled host responded or refused is a downstream outage, not a frontend problem, so it does not fail.
func (c *peerHealthChecker) evaluateRound(
	plannedPerRole map[primitives.ServiceName]int,
	counts map[primitives.ServiceName]map[peerProbeOutcome]int,
	representativeErrors map[primitives.ServiceName]error,
) string {
	threshold := c.failureThreshold()
	minFailedHosts := c.minFailedHosts()
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
		timeouts := roleCounts[peerProbeTimeout]
		logRole := func(msg string) {
			tags := []tag.Tag{
				tag.NewStringTag("target-service", string(role)),
				tag.NewInt("planned", planned),
				tag.NewInt("timeout", timeouts),
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
		if meetsFailureThreshold(timeouts) {
			logRole("peer health check role failed")
			verdict = peerRoundFail
		} else if verdict == peerRoundPass && meetsFailureThreshold(timeouts+roleCounts[peerProbeUnknown]) {
			// The unfinished or inconclusive probes could still change the verdict.
			verdict = peerRoundUnknown
		}
	}
	return verdict
}

// Start launches the check loop. It is idempotent.
func (c *peerHealthChecker) Start() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
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
	if atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusInitialized, common.DaemonStatusStopped) {
		return
	}
	if !atomic.CompareAndSwapInt32(&c.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
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

// probeTimeout stays below the round timeout: the round cuts off a longer probe as unknown, so a stuck host could never count as timed out.
func (c *peerHealthChecker) probeTimeout() time.Duration {
	round := c.roundTimeout()
	if d := c.config.Settings().ProbeTimeout; d > 0 && d < round {
		return d
	}
	return min(dynamicconfig.DefaultPeerHealthCheckSettings.ProbeTimeout, round/2)
}

func (c *peerHealthChecker) roundTimeout() time.Duration {
	if d := c.config.Settings().RoundTimeout; d > 0 {
		return d
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.RoundTimeout
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

func (c *peerHealthChecker) maxHostsPerRole() int {
	if n := c.config.Settings().MaxHostsPerRole; n > 0 {
		return n
	}
	return dynamicconfig.DefaultPeerHealthCheckSettings.MaxHostsPerRole
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
