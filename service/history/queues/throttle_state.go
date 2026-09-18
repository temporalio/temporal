package queues

import (
	"math"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

const (
	throttleSweepDivisor = 4

	defaultThrottleBeta          = 0.85
	defaultThrottleIncreaseRatio = 0.10
	defaultThrottleLossThreshold = 0.05
	defaultThrottleWindow        = time.Second
	defaultThrottleMaxKeys       = 1024
	defaultThrottleMinRate       = 1.0
	defaultThrottleMaxRate       = 10000.0
	defaultThrottleInitialRate   = 1000.0
	defaultThrottleKeyTTL        = 5 * time.Minute
)

type (
	// ThrottleKey identifies one controlled class: one bucket per budget. Priority is not part
	// of it. The rescheduler offers that one bucket to classes in priority order, which only
	// means something while they are drawing on the same budget.
	ThrottleKey struct {
		Cause       enumspb.ResourceExhaustedCause
		NamespaceID string
	}

	ThrottleStateOptions struct {
		Enabled       dynamicconfig.BoolPropertyFn
		MinRate       dynamicconfig.FloatPropertyFn
		MaxRate       dynamicconfig.FloatPropertyFn
		InitialRate   dynamicconfig.FloatPropertyFn
		KeyTTL        dynamicconfig.DurationPropertyFn
		Beta          dynamicconfig.FloatPropertyFn
		IncreaseRatio dynamicconfig.FloatPropertyFn
		LossThreshold dynamicconfig.FloatPropertyFn
		Window        dynamicconfig.DurationPropertyFn
		MaxKeys       dynamicconfig.IntPropertyFn
	}

	ThrottleState struct {
		options        ThrottleStateOptions
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler

		mu        sync.RWMutex
		entries   map[ThrottleKey]*throttleEntry
		lastSweep time.Time
	}

	throttleEntry struct {
		key ThrottleKey

		sync.Mutex
		rate         float64
		tokens       float64
		lastRefill   time.Time
		windowStart  time.Time
		lastAccess   time.Time
		releases     int64
		rejections   int64
		suppressions int64
	}
)

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
		entries:        make(map[ThrottleKey]*throttleEntry),
	}
	return s
}

func IsControllerInput(
	cause enumspb.ResourceExhaustedCause,
	scope enumspb.ResourceExhaustedScope,
) bool {
	if scope != enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE {
		return false
	}
	return cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT ||
		cause == enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT
}

func NewThrottleKey(cause enumspb.ResourceExhaustedCause, namespaceID string) ThrottleKey {
	return ThrottleKey{Cause: cause, NamespaceID: namespaceID}
}

func (k ThrottleKey) metricsTags() []metrics.Tag {
	return []metrics.Tag{
		metrics.ResourceExhaustedCauseTag(k.Cause),
		metrics.NamespaceIDTag(k.NamespaceID),
	}
}

// cappedTags omits the unbounded namespace dimension after the key cap is reached.
func (k ThrottleKey) cappedTags() []metrics.Tag {
	return []metrics.Tag{metrics.ResourceExhaustedCauseTag(k.Cause)}
}

func (s *ThrottleState) Enabled() bool {
	return s != nil && s.options.Enabled != nil && s.options.Enabled()
}

func (s *ThrottleState) Window() time.Duration {
	return configured(s.options.Window, defaultThrottleWindow)
}

func (s *ThrottleState) Admit(key ThrottleKey) (allowed, metered bool, retryAfter time.Duration) {
	if !s.Enabled() {
		return true, false, 0
	}
	entry := s.getOrCreate(key)
	if entry == nil {
		metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.cappedTags()...)
		return true, false, 0
	}

	now := s.timeSource.Now()
	window := s.Window()
	entry.Lock()
	defer entry.Unlock()

	s.touchLocked(entry, now, window)
	s.advanceWindowLocked(entry, now, window)
	entry.refillLocked(now, window)
	if entry.tokens < 1 {
		entry.suppressions++
		metrics.TaskThrottleGateSuppressed.With(s.metricsHandler).Record(1, key.metricsTags()...)
		return false, false, entry.tokenETALocked()
	}

	entry.tokens--
	entry.releases++
	metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
	return true, true, 0
}

// Return gives back a release the scheduler refused. It never reached the enforcer, so it is
// not a rejection, and leaving it counted would read as a clean release and raise the rate.
func (s *ThrottleState) Return(key ThrottleKey) {
	entry := s.peek(key)
	if entry == nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()

	entry.tokens = min(entry.tokens+1, entry.burstLocked(s.Window()))
	if entry.releases > 0 {
		entry.releases--
	}
}

// ReportThrottled feeds one rejection in. metered says the gate issued the release it refused,
// which is what makes it evidence: loss on traffic the controller never sent would drive a
// class to the floor while the traffic actually consuming the budget flowed past.
//
// A metered rejection is recorded even when the controller has since been turned off, because
// the release it answers was already counted and the window would otherwise read clean.
func (s *ThrottleState) ReportThrottled(key ThrottleKey, metered bool) {
	if !s.Enabled() && !metered {
		return
	}
	entry := s.peek(key)
	if metered && entry == nil {
		entry = s.getOrCreate(key)
	}
	if entry == nil {
		metrics.TaskThrottleRejections.With(s.metricsHandler).Record(1, key.cappedTags()...)
		return
	}
	metrics.TaskThrottleRejections.With(s.metricsHandler).Record(1, key.metricsTags()...)

	now := s.timeSource.Now()
	window := s.Window()
	entry.Lock()
	defer entry.Unlock()

	s.touchLocked(entry, now, window)
	if metered {
		entry.rejections++
		s.advanceWindowLocked(entry, now, window)
	}
}

// minDecisionReleases is how many releases a loss ratio needs before it can resolve the
// threshold. Below 1/threshold the smallest non-zero ratio is already above it, so a single
// rejection would decide "total loss" for a class that is merely slow.
func minDecisionReleases(lossThreshold float64) int64 {
	if !(lossThreshold > 0) {
		return 1
	}
	return int64(math.Ceil(1 / lossThreshold))
}

// advanceWindowLocked closes an elapsed window and applies at most one rate change for it.
//
// A window that carries too little evidence to resolve the threshold is closed without a
// decision and its counters are carried forward, so a low rate class accumulates a measurable
// sample instead of reacting to the first rejection it sees.
func (s *ThrottleState) advanceWindowLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if now.Sub(entry.windowStart) < window {
		return
	}
	// Credit the elapsed window at the rate that governed it, before a decision changes it.
	entry.refillLocked(now, window)
	entry.windowStart = now

	beta, increaseRatio, lossThreshold := s.controlLaw()
	minSamples := minDecisionReleases(lossThreshold)
	if entry.releases < minSamples {
		return
	}
	defer func() {
		entry.releases, entry.rejections, entry.suppressions = 0, 0, 0
	}()

	// A rejection can land in the window after the one that released it, so this can exceed
	// 1. It is only ever compared to the threshold, which it is above either way.
	loss := float64(entry.rejections) / float64(entry.releases)
	switch {
	case loss > lossThreshold:
		entry.rate = s.clamp(entry.rate * beta)
		// Tokens banked at the old rate would let the class overshoot the new one.
		entry.tokens = min(entry.tokens, entry.burstLocked(window))
		metrics.TaskThrottleRateDecreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	case entry.suppressions > 0:
		entry.rate = s.clamp(entry.rate * (1 + increaseRatio))
		metrics.TaskThrottleRateIncreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	default:
		// The gate never refused this class, so it has not asked for a higher rate. Raising it
		// anyway would grow the burst it can spend the moment demand returns.
		return
	}
	metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, entry.key.metricsTags()...)
}

func (e *throttleEntry) resetLocked(rate float64, now time.Time, window time.Duration) {
	e.rate = rate
	e.lastRefill = now
	e.windowStart = now
	e.releases, e.rejections, e.suppressions = 0, 0, 0
	e.tokens = e.burstLocked(window)
}

func (s *ThrottleState) touchLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if !entry.lastAccess.IsZero() && now.Sub(entry.lastAccess) > s.ttl() {
		entry.resetLocked(s.clamp(s.startRate()), now, window)
	}
	// A backwards clock step must not make an active entry look idle.
	if now.After(entry.lastAccess) {
		entry.lastAccess = now
	}
}

func (e *throttleEntry) refillLocked(now time.Time, window time.Duration) {
	elapsed := now.Sub(e.lastRefill)
	if elapsed <= 0 {
		return
	}
	e.lastRefill = now
	e.tokens = min(e.tokens+e.rate*elapsed.Seconds(), e.burstLocked(window))
}

func (e *throttleEntry) burstLocked(window time.Duration) float64 {
	return max(1, e.rate*window.Seconds())
}

func (e *throttleEntry) tokenETALocked() time.Duration {
	deficit := 1 - e.tokens
	if deficit <= 0 || e.rate <= 0 {
		return 0
	}
	seconds := deficit / e.rate
	if seconds > math.MaxInt64/float64(time.Second) {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

func (s *ThrottleState) clamp(rate float64) float64 {
	if math.IsNaN(rate) {
		return s.floor()
	}
	return min(max(rate, s.floor()), s.ceiling())
}

// Every knob is live dynamic config, and every one falls back to its documented default when
// it is unset or set to something the control law cannot use. These are the two shapes that
// takes: a positive value, or a fraction strictly between zero and one.
func configured[T ~float64 | ~int64 | ~int](fn func() T, fallback T) T {
	if fn != nil {
		if value := fn(); value > 0 {
			return value
		}
	}
	return fallback
}

// A gain outside (0, 1) inverts the control law rather than tuning it: a decrease factor of 1
// or more raises the rate on loss, and a zero increase never lifts it again.
func configuredFraction(fn dynamicconfig.FloatPropertyFn, fallback float64) float64 {
	if fn != nil {
		if value := fn(); value > 0 && value < 1 {
			return value
		}
	}
	return fallback
}

func (s *ThrottleState) floor() float64 {
	return configured(s.options.MinRate, defaultThrottleMinRate)
}

func (s *ThrottleState) ceiling() float64 {
	return configured(s.options.MaxRate, defaultThrottleMaxRate)
}

func (s *ThrottleState) startRate() float64 {
	return configured(s.options.InitialRate, defaultThrottleInitialRate)
}

func (s *ThrottleState) ttl() time.Duration {
	return configured(s.options.KeyTTL, defaultThrottleKeyTTL)
}

func (s *ThrottleState) maxKeys() int {
	return configured(s.options.MaxKeys, defaultThrottleMaxKeys)
}

func (s *ThrottleState) controlLaw() (beta, increaseRatio, lossThreshold float64) {
	return configuredFraction(s.options.Beta, defaultThrottleBeta),
		configuredFraction(s.options.IncreaseRatio, defaultThrottleIncreaseRatio),
		configuredFraction(s.options.LossThreshold, defaultThrottleLossThreshold)
}

func (s *ThrottleState) peek(key ThrottleKey) *throttleEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[key]
}

func (s *ThrottleState) getOrCreate(key ThrottleKey) *throttleEntry {
	if entry := s.peek(key); entry != nil {
		return entry
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if entry, ok := s.entries[key]; ok {
		return entry
	}

	now := s.timeSource.Now()
	s.maybeSweepLocked(now)
	if len(s.entries) >= s.maxKeys() {
		// Fail open past the cap. TaskThrottleKeysDropped is the signal; a log line per
		// dropped key would fire hardest exactly when the host is already struggling.
		metrics.TaskThrottleKeysDropped.With(s.metricsHandler).Record(1, key.cappedTags()...)
		return nil
	}

	entry := &throttleEntry{
		key:         key,
		rate:        s.clamp(s.startRate()),
		lastRefill:  now,
		windowStart: now,
		lastAccess:  now,
	}
	entry.tokens = entry.burstLocked(s.Window())
	s.entries[key] = entry
	metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(float64(len(s.entries)))
	return entry
}

func (s *ThrottleState) maybeSweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < s.ttl()/throttleSweepDivisor {
		return
	}
	s.lastSweep = now
	evicted := false
	for key, entry := range s.entries {
		entry.Lock()
		idle := now.Sub(entry.lastAccess) > s.ttl()
		entry.Unlock()
		if idle {
			delete(s.entries, key)
			evicted = true
			metrics.TaskThrottleKeysEvicted.With(s.metricsHandler).Record(1, key.metricsTags()...)
		}
	}
	if evicted {
		metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(float64(len(s.entries)))
	}
}
