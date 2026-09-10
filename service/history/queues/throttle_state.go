package queues

import (
	"errors"
	"math"
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

// Sweep a few times per TTL so eviction keeps up with churn without a dedicated goroutine.
const throttleSweepDivisor = 4

// Matches the dynamic config default, so a bad push lands on the documented value.
const defaultThrottleMaxKeys = 1024

// Guardrails rather than tuning knobs: none has a production story that would justify the
// dynamic config surface, and a pushed MinRate of 0 would stall every class. Tests override
// them through ThrottleStateOptions.
const (
	defaultThrottleMinRate     = 1.0
	defaultThrottleMaxRate     = 10000.0
	defaultThrottleInitialRate = 1000.0
	defaultThrottleKeyTTL      = 5 * time.Minute
)

type (
	// ThrottleController paces releases for a class of parked tasks. ThrottleState is the AIMD
	// implementation; the interface exists so a different control law can replace it without
	// touching the rescheduler or the executable.
	//
	// Implementations must tolerate being called concurrently for the same key.
	ThrottleController interface {
		// Enabled reports whether releases are being gated at all. Callers check this before
		// anything else, so it must be safe on a controller that was never constructed.
		Enabled() bool

		// Window is the control period. The rescheduler needs it to bound how long it will
		// wait on a denied class.
		Window() time.Duration

		// Admit consumes one release token. metered says the controller counted this release
		// as its own, which is what makes a later rejection evidence it may act on.
		// retryAfter, set only on denial, is how long until a token is expected.
		Admit(key ThrottleKey) (allowed, metered bool, retryAfter time.Duration)

		// Return gives back a token for a release that never happened.
		Return(key ThrottleKey)

		// ReportThrottled feeds one observed rejection in. admitted must be the metered value
		// from the Admit that issued the release.
		ReportThrottled(key ThrottleKey, admitted bool)

		// ReportSuccess reports that a released task completed.
		ReportSuccess(key ThrottleKey)
	}

	// ThrottleKey identifies one controlled class. Every governed cause is a namespace budget,
	// so one budget is one class however the traffic is spread.
	ThrottleKey struct {
		Cause       enumspb.ResourceExhaustedCause
		NamespaceID string
	}

	// controlLaw is the part of the configuration a rate decision needs. It is read only when
	// a window actually closes, which is once per window per class rather than once per admit.
	controlLaw struct {
		beta          float64
		increaseRatio float64
		lossThreshold float64
	}

	// ThrottleStateOptions are the AIMD control law parameters. The property functions are live
	// dynamic config; the plain fields are fixed guardrails that only tests set, and zero means
	// take the default.
	ThrottleStateOptions struct {
		Enabled       dynamicconfig.BoolPropertyFn
		Beta          dynamicconfig.FloatPropertyFn
		IncreaseRatio dynamicconfig.FloatPropertyFn
		LossThreshold dynamicconfig.FloatPropertyFn
		Window        dynamicconfig.DurationPropertyFn
		MaxKeys       dynamicconfig.IntPropertyFn

		MinRate     float64
		MaxRate     float64
		InitialRate float64
		KeyTTL      time.Duration
	}

	// ThrottleState is the host level throttle controller. It turns per-task throttle
	// rejections into a per-class admitted rate, which the rescheduler spends as a release
	// budget instead of letting every parked task rediscover the same constraint.
	ThrottleState struct {
		options        ThrottleStateOptions
		timeSource     clock.TimeSource
		logger         log.Logger
		metricsHandler metrics.Handler

		// Resolved once at construction. Reading them per call would cost a branch on a path
		// that runs thousands of times a second for no benefit; they cannot change at runtime.
		minRate     float64
		maxRate     float64
		initialRate float64
		keyTTL      time.Duration

		mu         sync.RWMutex
		entries    map[ThrottleKey]*throttleEntry
		lastSweep  time.Time
		lastCapLog time.Time
	}

	throttleEntry struct {
		key ThrottleKey

		sync.Mutex
		rate        float64
		tokens      float64
		lastRefill  time.Time
		windowStart time.Time
		lastAccess  time.Time
		// Evidence for the open window. Both scale with what the class released, which is what
		// keeps the settling rate a function of its own demand rather than of the enforcer's
		// background rejection probability.
		releases   int64
		rejections int64
		decreases  int64
		increases  int64
	}
)

// The causes the controller governs. Pacing only helps when the budget is shared across tasks
// and this namespace owns it, which is why system scoped instances are excluded.
//
// PERSISTENCE_LIMIT covers two enforcement points that report identically, so a per shard
// limiter's rejections pace the whole namespace on this host. Telling them apart needs a
// distinguishable error.
var controlledCauses = map[enumspb.ResourceExhaustedCause]struct{}{
	enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT:         {},
	enumspb.RESOURCE_EXHAUSTED_CAUSE_PERSISTENCE_LIMIT: {},
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
		minRate:        defaultThrottleMinRate,
		maxRate:        defaultThrottleMaxRate,
		initialRate:    defaultThrottleInitialRate,
		keyTTL:         defaultThrottleKeyTTL,
		lastSweep:      timeSource.Now(),
		entries:        make(map[ThrottleKey]*throttleEntry),
	}
	if options.MinRate > 0 {
		s.minRate = options.MinRate
	}
	if options.MaxRate >= s.minRate {
		s.maxRate = options.MaxRate
	}
	if s.maxRate < s.minRate {
		s.maxRate = s.minRate
	}
	if options.InitialRate > 0 {
		s.initialRate = options.InitialRate
	}
	if options.KeyTTL > 0 {
		s.keyTTL = options.KeyTTL
	}
	return s
}

// IsControllerInput reports whether a rejection should drive the controller.
//
// ErrBusinessIDRateLimitExceeded claims namespace scope but is enforced per business ID, so one
// hot workflow would ratchet the whole namespace down.
func IsControllerInput(
	err error,
	cause enumspb.ResourceExhaustedCause,
	scope enumspb.ResourceExhaustedScope,
) bool {
	if errors.Is(err, consts.ErrBusinessIDRateLimitExceeded) {
		return false
	}
	if scope != enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE {
		return false
	}
	_, ok := controlledCauses[cause]
	return ok
}

func NewThrottleKey(cause enumspb.ResourceExhaustedCause, namespaceID string) ThrottleKey {
	return ThrottleKey{
		Cause:       cause,
		NamespaceID: namespaceID,
	}
}

func (k ThrottleKey) metricsTags() []metrics.Tag {
	tags := []metrics.Tag{metrics.ResourceExhaustedCauseTag(k.Cause)}
	if k.NamespaceID != "" {
		tags = append(tags, metrics.NamespaceIDTag(k.NamespaceID))
	}
	return tags
}

// cappedTags omits the namespace: past the cap that population is unbounded, and tagging by it
// would move the cardinality the cap exists to prevent into the metrics pipeline.
func (k ThrottleKey) cappedTags() []metrics.Tag {
	return []metrics.Tag{metrics.ResourceExhaustedCauseTag(k.Cause)}
}

// Enabled tolerates a nil receiver. A controller that was never constructed reaches the
// callers as a typed nil inside a ThrottleController, which is not == nil, so without this the
// first check on a server with the feature unwired would panic instead of reading as disabled.
func (s *ThrottleState) Enabled() bool {
	return s != nil && s.options.Enabled != nil && s.options.Enabled()
}

// Admit fails open when disabled or past the key cap, leaving the real limiter as the enforcer.
// An unmetered release is loss on a packet the controller never sent, so its rejection must not
// move the rate.
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
		metrics.TaskThrottleGateSuppressed.With(s.metricsHandler).Record(1, key.metricsTags()...)
		return false, false, entry.tokenETALocked()
	}
	entry.tokens--
	entry.releases++
	metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
	return true, true, 0
}

// ReportThrottled feeds one observed rejection into the controller.
//
// admitted says the attempt came from a release this gate issued. Only those close the loop:
// reacting to traffic it never sent would ratchet parked tasks to the floor while the traffic
// actually consuming the budget flows past untouched.
func (s *ThrottleState) ReportThrottled(key ThrottleKey, admitted bool) {
	if !s.Enabled() {
		return
	}
	entry := s.getOrCreate(key)
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
	if !admitted {
		return
	}
	// Recorded only. The rate moves once per window, so a class at its budget cannot ratchet
	// itself down by rejecting routinely.
	entry.rejections++
	s.advanceWindowLocked(entry, now, window)
}

// Return gives back a token taken by Admit for a release that never happened, so scheduler
// saturation does not silently spend the class's budget.
func (s *ThrottleState) Return(key ThrottleKey) {
	if !s.Enabled() {
		return
	}
	entry := s.peek(key)
	if entry == nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()

	entry.tokens = min(entry.tokens+1, entry.burstLocked(s.Window()))
	// Leaves the loss denominator too. A dispatch that never happened cannot be rejected, so
	// keeping it would read as a guaranteed success and climb the rate on nothing.
	if entry.releases > 0 {
		entry.releases--
	}
}

// ReportSuccess keeps a class that is completing work from being swept as idle, and closes an
// elapsed window. It cannot raise an idle class, since a window with no releases is left alone;
// what it does reach is the last window of a drain, after admit stops being called.
func (s *ThrottleState) ReportSuccess(key ThrottleKey) {
	if !s.Enabled() {
		return
	}
	entry := s.peek(key)
	if entry == nil {
		return
	}

	now := s.timeSource.Now()
	window := s.Window()

	entry.Lock()
	defer entry.Unlock()

	s.touchLocked(entry, now, window)
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

// Len returns the number of tracked keys.
func (s *ThrottleState) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// advanceWindowLocked closes an elapsed window and applies one rate change for it. The decision
// is the loss ratio rather than any single rejection, which is what keeps the settling rate
// independent of class size. A class that released nothing is left alone.
func (s *ThrottleState) advanceWindowLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if now.Sub(entry.windowStart) < window {
		return
	}
	// Credit the elapsed window at the rate that governed it. A decision made now applies to
	// the time after now; crediting at the new rate hands out tokens the class never earned.
	entry.refillLocked(now, window)
	defer func() {
		entry.windowStart = now
		entry.releases, entry.rejections = 0, 0
	}()

	if entry.releases == 0 && entry.rejections == 0 {
		return
	}

	// A rejection can land in the window after the one that released it, so bound the ratio at
	// 1 rather than letting the skew read as loss above 100%.
	loss := 1.0
	if entry.releases > entry.rejections {
		loss = float64(entry.rejections) / float64(entry.releases)
	}

	law := s.controlLaw()
	if loss > law.lossThreshold {
		entry.rate = s.clamp(entry.rate * law.beta)
		entry.decreases++
		// Tokens banked at the old rate would let the class overshoot the new one.
		entry.tokens = min(entry.tokens, entry.burstLocked(window))
		metrics.TaskThrottleRateDecreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	} else {
		entry.rate = s.clamp(entry.rate * (1 + law.increaseRatio))
		entry.increases++
		metrics.TaskThrottleRateIncreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	}
	metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, entry.key.metricsTags()...)
}

// resetLocked discards everything the control law learned, keeping the entry's identity.
func (e *throttleEntry) resetLocked(rate float64, now time.Time, window time.Duration) {
	e.rate = rate
	e.lastRefill = now
	e.windowStart = now
	e.releases, e.rejections = 0, 0
	e.tokens = e.burstLocked(window)
}

// touchLocked marks the entry live, resetting it first if it has been idle past the TTL. The
// sweep only runs on insert, and a host's key set goes stable early, so without this a class
// driven to the floor by an incident would climb back from MinRate rather than restart.
func (s *ThrottleState) touchLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if !entry.lastAccess.IsZero() && now.Sub(entry.lastAccess) > s.keyTTL {
		entry.resetLocked(s.clamp(s.initialRate), now, window)
	}
	// Forward only: a clock stepped backwards would make the recovery look like a full TTL of
	// inactivity and reset a busy class.
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

// burstLocked caps credit at one window's worth, so an idle class cannot dump its backlog the
// moment it becomes due.
func (e *throttleEntry) burstLocked(window time.Duration) float64 {
	return max(1, e.rate*window.Seconds())
}

// tokenETALocked is how long until the bucket holds a whole token. Zero means no refill will
// satisfy it. The estimate expires at the next window close, where the rate moves, so callers
// are expected to cap it there.
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

// Window is read on every admit, so it stays a single lookup. A non positive window would close
// on every call and let one release and its rejection be decided twice.
func (s *ThrottleState) Window() time.Duration {
	if w := s.options.Window(); w > 0 {
		return w
	}
	return time.Second
}

// controlLaw is read only when a window closes. Each floor exists so a bad config push stalls
// the controller rather than inverting it: a beta at or above 1 would raise the rate on loss,
// and an increase ratio at or below 0 would lower it on success.
//
// The guards are negated on purpose: NaN compares false against every bound, so !(x > 0) catches
// it where x <= 0 would let it through.
func (s *ThrottleState) controlLaw() controlLaw {
	c := controlLaw{
		beta:          s.options.Beta(),
		increaseRatio: s.options.IncreaseRatio(),
		lossThreshold: s.options.LossThreshold(),
	}
	if !(c.beta > 0 && c.beta < 1) {
		c.beta = 1
	}
	if !(c.increaseRatio > 0) {
		c.increaseRatio = 0
	}
	if !(c.lossThreshold >= 0) {
		c.lossThreshold = 0
	}
	c.lossThreshold = min(c.lossThreshold, 1)
	return c
}

// maxKeys is read only when a key is created. At zero the cap is already met by an empty map,
// so every class would fail open while the controller still reported itself enabled.
func (s *ThrottleState) maxKeys() int {
	if n := s.options.MaxKeys(); n > 0 {
		return n
	}
	return defaultThrottleMaxKeys
}

// clamp holds a rate inside the fixed band. NaN survives min and max, and NaN tokens make the
// tokens < 1 test false forever, so one bad value would open the gate permanently.
func (s *ThrottleState) clamp(rate float64) float64 {
	if math.IsNaN(rate) {
		return s.minRate
	}
	return min(max(rate, s.minRate), s.maxRate)
}

func (s *ThrottleState) peek(key ThrottleKey) *throttleEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries[key]
}

// getOrCreate creates entries lazily, returning nil at the key cap for callers to fail open on.
//
// The entry is not pinned, so a concurrent sweep can orphan it. That costs at most one extra
// release, and only for a key idle past its TTL, which is a class under no pressure.
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
	s.maybeSweepLocked(now, s.keyTTL)

	if len(s.entries) >= s.maxKeys() {
		metrics.TaskThrottleKeysDropped.With(s.metricsHandler).Record(1, key.cappedTags()...)
		if now.Sub(s.lastCapLog) >= s.keyTTL {
			s.lastCapLog = now
			s.logger.Warn("Throttle controller key cap reached, failing open.",
				tag.NewStringTag("throttle-cause", key.Cause.String()),
			)
		}
		return nil
	}

	entry := &throttleEntry{
		key:         key,
		rate:        s.clamp(s.initialRate),
		lastRefill:  now,
		windowStart: now,
		lastAccess:  now,
	}
	entry.tokens = entry.burstLocked(s.Window())
	s.entries[key] = entry
	metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(float64(len(s.entries)))
	return entry
}

// maybeSweepLocked evicts idle keys inline on insert, so ThrottleState needs no lifecycle.
func (s *ThrottleState) maybeSweepLocked(now time.Time, ttl time.Duration) {
	if now.Sub(s.lastSweep) < ttl/throttleSweepDivisor {
		return
	}
	s.lastSweep = now
	for key, entry := range s.entries {
		entry.Lock()
		idle := now.Sub(entry.lastAccess) > ttl
		entry.Unlock()
		if idle {
			delete(s.entries, key)
			metrics.TaskThrottleKeysEvicted.With(s.metricsHandler).Record(1, key.metricsTags()...)
		}
	}
}
