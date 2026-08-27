package queues

import (
	"errors"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/consts"
)

type (
	// ThrottleScope identifies which population a throttle applies to, and therefore
	// which of the ThrottleState maps holds its control state.
	ThrottleScope int
)

const (
	// ThrottleScopeHost covers causes enforced for the whole process (system overload,
	// system-wide persistence budgets, storage limits).
	ThrottleScopeHost ThrottleScope = iota
	// ThrottleScopeNamespace covers per-namespace budgets that are enforced identically
	// on every host and every shard.
	ThrottleScopeNamespace
	// ThrottleScopeNamespaceShard covers per-namespace budgets whose enforcement point is
	// the shard owner, so two shards for the same namespace converge independently.
	ThrottleScopeNamespaceShard
)

const numThrottleScopes = 3

// throttleSweepDivisor makes the lazy TTL sweep run a few times per TTL rather than once,
// so eviction keeps up with churn without a dedicated goroutine.
const throttleSweepDivisor = 4

type (
	// ThrottleKey identifies one controlled class. Category is populated only for
	// infrastructure causes: a Cassandra, Elasticsearch or matching overload observed by one
	// task category says nothing about a category that never touches that dependency.
	// Namespace budget causes deliberately leave Category empty so every category shares
	// the single enforced budget.
	ThrottleKey struct {
		Scope       ThrottleScope
		Cause       enumspb.ResourceExhaustedCause
		NamespaceID string
		ShardID     int32
		Category    string
	}

	// ThrottleStateOptions are the AIMD control law parameters.
	ThrottleStateOptions struct {
		Enabled       dynamicconfig.BoolPropertyFn
		Beta          dynamicconfig.FloatPropertyFn
		IncreaseRatio dynamicconfig.FloatPropertyFn
		Window        dynamicconfig.DurationPropertyFn
		MinRate       dynamicconfig.FloatPropertyFn
		MaxRate       dynamicconfig.FloatPropertyFn
		InitialRate   dynamicconfig.FloatPropertyFn
		MaxKeys       dynamicconfig.IntPropertyFn
		KeyTTL        dynamicconfig.DurationPropertyFn
	}

	// ThrottleState is the host level throttle controller. It converts per-task throttle
	// rejections into a per-class admitted rate using AIMD, and hands that rate back to the
	// rescheduler as a release budget so that N parked tasks stop rediscovering the same
	// constraint N times per backoff round.
	ThrottleState struct {
		options        ThrottleStateOptions
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler

		mu         sync.RWMutex
		maps       [numThrottleScopes]map[ThrottleKey]*throttleEntry
		lastSweep  time.Time
		lastCapLog [numThrottleScopes]time.Time
	}

	throttleEntry struct {
		key ThrottleKey

		sync.Mutex
		rate         float64
		tokens       float64
		lastRefill   time.Time
		windowStart  time.Time
		lastThrottle time.Time
		lastDecrease time.Time
		lastAccess   time.Time
		decreases    int64
		increases    int64
	}
)

// namespaceBudgetCauses are enforced against a namespace's own quota. They are keyed without
// the task category because every category draws from the same token bucket.
var namespaceBudgetCauses = map[enumspb.ResourceExhaustedCause]struct{}{
	enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT:         {},
	enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT:         {},
	enumspb.RESOURCE_EXHAUSTED_CAUSE_OPS_LIMIT:         {},
	enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT: {},
}

// shardScopedCauses are namespace budgets whose enforcement point is the shard owner.
// CONSIDER(ppv): persistence per-shard namespace limits belong here too, but they report the
// same (cause, scope) pair as the host wide namespace limit and cannot be told apart yet.
var shardScopedCauses = map[enumspb.ResourceExhaustedCause]struct{}{
	enumspb.RESOURCE_EXHAUSTED_CAUSE_CONCURRENT_LIMIT: {},
}

func NewThrottleState(
	options ThrottleStateOptions,
	timeSource clock.TimeSource,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ThrottleState {
	s := &ThrottleState{
		options:        options,
		timeSource:     timeSource,
		logger:         logger,
		metricsHandler: metricsHandler,
		lastSweep:      timeSource.Now(),
	}
	for i := range s.maps {
		s.maps[i] = make(map[ThrottleKey]*throttleEntry)
	}
	return s
}

// IsControllerInput reports whether a rejection should drive the controller. Only rejections
// from a budget shared across tasks qualify: the control law reacts by slowing a whole class,
// which is pointless when the contended resource belongs to one workflow.
//
// BUSY_WORKFLOW is per-workflow lock contention and already has a dedicated fast path.
// CIRCUIT_BREAKER_OPEN is itself a controller and would double-govern.
// ErrBusinessIDRateLimitExceeded reports RPS_LIMIT at namespace scope but is enforced
// per (namespace, businessID, archetype), so one hot workflow ID would otherwise ratchet the
// namespace wide class down and gate every unrelated task in that namespace on the host.
func IsControllerInput(err error, cause enumspb.ResourceExhaustedCause) bool {
	if errors.Is(err, consts.ErrBusinessIDRateLimitExceeded) {
		return false
	}
	switch cause { //nolint:exhaustive
	case enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW,
		enumspb.RESOURCE_EXHAUSTED_CAUSE_CIRCUIT_BREAKER_OPEN:
		return false
	default:
		return true
	}
}

// NewThrottleKey routes a reported (cause, scope) onto the class that shares its enforcement point.
func NewThrottleKey(
	cause enumspb.ResourceExhaustedCause,
	scope enumspb.ResourceExhaustedScope,
	namespaceID string,
	shardID int32,
	category string,
) ThrottleKey {
	_, isNamespaceBudget := namespaceBudgetCauses[cause]
	switch {
	case scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE && isNamespaceBudget:
		if _, shardScoped := shardScopedCauses[cause]; shardScoped {
			return ThrottleKey{
				Scope:       ThrottleScopeNamespaceShard,
				Cause:       cause,
				NamespaceID: namespaceID,
				ShardID:     shardID,
			}
		}
		return ThrottleKey{
			Scope:       ThrottleScopeNamespace,
			Cause:       cause,
			NamespaceID: namespaceID,
		}
	case scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE:
		// A namespace scoped report for an infrastructure cause means one namespace's traffic
		// is overloading a dependency on this shard; keep it off the host wide key.
		return ThrottleKey{
			Scope:       ThrottleScopeNamespaceShard,
			Cause:       cause,
			NamespaceID: namespaceID,
			ShardID:     shardID,
			Category:    category,
		}
	case isNamespaceBudget && scope == enumspb.RESOURCE_EXHAUSTED_SCOPE_UNSPECIFIED:
		return ThrottleKey{
			Scope:       ThrottleScopeNamespace,
			Cause:       cause,
			NamespaceID: namespaceID,
		}
	default:
		return ThrottleKey{
			Scope:    ThrottleScopeHost,
			Cause:    cause,
			Category: category,
		}
	}
}

func (k ThrottleKey) metricsTags() []metrics.Tag {
	tags := []metrics.Tag{
		metrics.ResourceExhaustedCauseTag(k.Cause),
		metrics.StringTag("throttle_scope", k.Scope.String()),
	}
	if k.NamespaceID != "" {
		tags = append(tags, metrics.NamespaceIDTag(k.NamespaceID))
	}
	if k.Category != "" {
		tags = append(tags, metrics.TaskCategoryTag(k.Category))
	}
	return tags
}

func (s ThrottleScope) String() string {
	switch s {
	case ThrottleScopeHost:
		return "host"
	case ThrottleScopeNamespace:
		return "namespace"
	case ThrottleScopeNamespaceShard:
		return "namespace_shard"
	default:
		return "unknown"
	}
}

// Enabled reports whether the controller is gating releases at all.
func (s *ThrottleState) Enabled() bool {
	return s.options.Enabled()
}

// Admit consumes one release token for the class. It returns true when the rescheduler may
// release a task for this class, and false when the class is over its currently admitted rate.
// It always returns true when the controller is disabled, and when the key cap has been hit,
// so that the real limiter, not this cache, stays the enforcement point.
func (s *ThrottleState) Admit(key ThrottleKey) bool {
	if !s.options.Enabled() {
		return true
	}
	entry := s.getOrCreate(key)
	if entry == nil {
		metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
		return true
	}

	now := s.timeSource.Now()
	window := s.options.Window()

	entry.Lock()
	defer entry.Unlock()

	entry.lastAccess = now
	s.advanceWindowLocked(entry, now, window)
	entry.refillLocked(now, window)

	if entry.tokens < 1 {
		metrics.TaskThrottleGateSuppressed.With(s.metricsHandler).Record(1, key.metricsTags()...)
		return false
	}
	entry.tokens--
	metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
	return true
}

// ReportThrottled feeds one observed throttle rejection into the controller.
//
// admitted says whether the attempt came from a release this gate issued. Only those close the
// loop. A rejection from an attempt the gate never metered - a task on its first dispatch
// straight from the queue reader - would otherwise decrease the rate every window forever and
// block every increase, ratcheting a busy namespace's parked tasks down to the floor while the
// traffic actually consuming the budget flows past untouched. This is TCP reacting to loss on
// packets it sent, not to loss it merely witnessed.
//
// At most one multiplicative decrease is applied per window: a namespace running at its budget
// rejects routinely, and reacting to every rejection ratchets the rate down to the floor.
func (s *ThrottleState) ReportThrottled(key ThrottleKey, admitted bool) {
	if !s.options.Enabled() {
		return
	}
	entry := s.getOrCreate(key)
	metrics.TaskThrottleRejections.With(s.metricsHandler).Record(1, key.metricsTags()...)
	if entry == nil {
		return
	}

	now := s.timeSource.Now()
	window := s.options.Window()

	entry.Lock()
	defer entry.Unlock()

	entry.lastAccess = now
	if !admitted {
		return
	}
	entry.lastThrottle = now
	if !entry.lastDecrease.IsZero() && now.Sub(entry.lastDecrease) < window {
		return
	}
	entry.rate = s.clamp(entry.rate * s.options.Beta())
	entry.lastDecrease = now
	entry.decreases++
	// Tokens accumulated at the old rate would otherwise let the class overshoot the new one.
	entry.tokens = min(entry.tokens, entry.burstLocked(window))

	metrics.TaskThrottleRateDecreases.With(s.metricsHandler).Record(1, key.metricsTags()...)
	metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, key.metricsTags()...)
}

// Return gives back a token taken by Admit for a release that never happened, so scheduler
// saturation does not silently spend the class's budget.
func (s *ThrottleState) Return(key ThrottleKey) {
	if !s.options.Enabled() {
		return
	}
	entry := s.peek(key)
	if entry == nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()

	entry.tokens = min(entry.tokens+1, entry.burstLocked(s.options.Window()))
}

// ReportSuccess records a completion for the class. It advances the window bookkeeping so a
// class that has stopped being throttled climbs back even while the rescheduler is idle.
func (s *ThrottleState) ReportSuccess(key ThrottleKey) {
	if !s.options.Enabled() {
		return
	}
	entry := s.peek(key)
	if entry == nil {
		return
	}

	now := s.timeSource.Now()
	window := s.options.Window()

	entry.Lock()
	defer entry.Unlock()

	entry.lastAccess = now
	s.advanceWindowLocked(entry, now, window)
}

// AdmittedRate returns the current admitted rate for a key, or 0 when it is not tracked.
func (s *ThrottleState) AdmittedRate(key ThrottleKey) float64 {
	entry := s.peek(key)
	if entry == nil {
		return 0
	}
	entry.Lock()
	defer entry.Unlock()
	return entry.rate
}

// Counters returns the number of decrease and increase events observed for a key.
func (s *ThrottleState) Counters(key ThrottleKey) (decreases int64, increases int64) {
	entry := s.peek(key)
	if entry == nil {
		return 0, 0
	}
	entry.Lock()
	defer entry.Unlock()
	return entry.decreases, entry.increases
}

// Len returns the number of tracked keys across all scopes.
func (s *ThrottleState) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	total := 0
	for i := range s.maps {
		total += len(s.maps[i])
	}
	return total
}

// advanceWindowLocked applies the additive increase when the window that just closed carried
// no throttle at all. The window start is reset to now rather than stepped forward, so a long
// idle period yields one increase, not one per elapsed window.
func (s *ThrottleState) advanceWindowLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if now.Sub(entry.windowStart) < window {
		return
	}
	if entry.lastThrottle.Before(entry.windowStart) {
		entry.rate = s.clamp(entry.rate * (1 + s.options.IncreaseRatio()))
		entry.increases++
		metrics.TaskThrottleRateIncreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
		metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, entry.key.metricsTags()...)
	}
	entry.windowStart = now
}

func (e *throttleEntry) refillLocked(now time.Time, window time.Duration) {
	elapsed := now.Sub(e.lastRefill)
	if elapsed <= 0 {
		return
	}
	e.lastRefill = now
	e.tokens = min(e.tokens+e.rate*elapsed.Seconds(), e.burstLocked(window))
}

// burstLocked caps accumulated credit at one window of the current rate so a class that was
// idle cannot dump its whole backlog the moment it becomes due.
func (e *throttleEntry) burstLocked(window time.Duration) float64 {
	return max(1, e.rate*window.Seconds())
}

func (s *ThrottleState) clamp(rate float64) float64 {
	return min(max(rate, s.options.MinRate()), s.options.MaxRate())
}

func (s *ThrottleState) peek(key ThrottleKey) *throttleEntry {
	if key.Scope < 0 || int(key.Scope) >= numThrottleScopes {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maps[key.Scope][key]
}

// getOrCreate returns the entry for a key, creating it lazily. It returns nil when the per map
// key cap is reached, which the callers treat as fail open: admit, and let the real limiter reject.
func (s *ThrottleState) getOrCreate(key ThrottleKey) *throttleEntry {
	if entry := s.peek(key); entry != nil {
		return entry
	}
	if key.Scope < 0 || int(key.Scope) >= numThrottleScopes {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if entry, ok := s.maps[key.Scope][key]; ok {
		return entry
	}

	now := s.timeSource.Now()
	s.maybeSweepLocked(now)

	if len(s.maps[key.Scope]) >= s.options.MaxKeys() {
		metrics.TaskThrottleKeysDropped.With(s.metricsHandler).Record(1, key.metricsTags()...)
		if now.Sub(s.lastCapLog[key.Scope]) >= s.options.KeyTTL() {
			s.lastCapLog[key.Scope] = now
			s.logger.Warn("Throttle controller key cap reached, failing open.",
				tag.NewStringTag("throttle-scope", key.Scope.String()),
				tag.NewStringTag("throttle-cause", key.Cause.String()),
			)
		}
		return nil
	}

	entry := &throttleEntry{
		key:         key,
		rate:        s.clamp(s.options.InitialRate()),
		lastRefill:  now,
		windowStart: now,
		lastAccess:  now,
	}
	entry.tokens = entry.burstLocked(s.options.Window())
	s.maps[key.Scope][key] = entry
	metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(
		float64(len(s.maps[key.Scope])),
		metrics.StringTag("throttle_scope", key.Scope.String()),
	)
	return entry
}

// maybeSweepLocked evicts idle keys. It runs inline on insert instead of from a goroutine so
// ThrottleState needs no lifecycle of its own.
func (s *ThrottleState) maybeSweepLocked(now time.Time) {
	ttl := s.options.KeyTTL()
	if now.Sub(s.lastSweep) < ttl/throttleSweepDivisor {
		return
	}
	s.lastSweep = now
	for i := range s.maps {
		for key, entry := range s.maps[i] {
			entry.Lock()
			idle := now.Sub(entry.lastAccess) > ttl
			entry.Unlock()
			if idle {
				delete(s.maps[i], key)
				metrics.TaskThrottleKeysEvicted.With(s.metricsHandler).Record(1, key.metricsTags()...)
			}
		}
	}
}
