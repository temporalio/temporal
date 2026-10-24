package queues

import (
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
)

const (
	throttleSweepDivisor = 4

	// A class below one release per second cannot earn the releases a decision needs, so it
	// would never climb back. Recovery is only bounded if the floor is.
	minThrottleRate = 1.0
)

type (
	// ThrottleKey is one bucket per budget. Priority is deliberately not part of it: the
	// rescheduler offers the one bucket to classes in priority order, which needs it shared.
	ThrottleKey struct {
		Cause       enumspb.ResourceExhaustedCause
		NamespaceID string
	}

	ThrottleState struct {
		settings       dynamicconfig.TypedPropertyFn[dynamicconfig.TaskThrottleControllerSettings]
		timeSource     clock.TimeSource
		metricsHandler metrics.Handler

		mu        sync.RWMutex
		entries   map[ThrottleKey]*throttleEntry
		lastSweep time.Time
	}

	throttleEntry struct {
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
	settings dynamicconfig.TypedPropertyFn[dynamicconfig.TaskThrottleControllerSettings],
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
) *ThrottleState {
	return &ThrottleState{
		settings:       settings,
		timeSource:     timeSource,
		metricsHandler: metricsHandler,
		lastSweep:      timeSource.Now(),
		entries:        make(map[ThrottleKey]*throttleEntry),
	}
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
	return s != nil && s.settings().Enabled
}

func (s *ThrottleState) Admit(key ThrottleKey) (allowed, metered bool) {
	if !s.Enabled() {
		return true, false
	}
	entry := s.getOrCreate(key)
	if entry == nil {
		metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.cappedTags()...)
		return true, false
	}

	now := s.timeSource.Now()
	window := s.settings().Window
	entry.Lock()
	defer entry.Unlock()

	s.touchLocked(entry, now, window)
	s.advanceWindowLocked(key, entry, now, window)
	entry.refillLocked(now, window)
	if entry.tokens < 1 {
		entry.suppressions++
		metrics.TaskThrottleGateSuppressed.With(s.metricsHandler).Record(1, key.metricsTags()...)
		return false, false
	}

	entry.tokens--
	entry.releases++
	metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
	return true, true
}

// Return takes back a release the scheduler refused. It never reached the enforcer, so
// leaving it counted would read as a clean one.
func (s *ThrottleState) Return(key ThrottleKey) {
	entry := s.peek(key)
	if entry == nil {
		return
	}

	entry.Lock()
	defer entry.Unlock()

	entry.tokens = min(entry.tokens+1, entry.burstLocked(s.settings().Window))
	if entry.releases > 0 {
		entry.releases--
	}
}

// ReportThrottled feeds one rejection in. metered says the gate issued the release that was
// refused, which is what makes it evidence. Such a rejection is recorded even once the
// controller is off, since the release it answers was already counted.
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
	window := s.settings().Window
	entry.Lock()
	defer entry.Unlock()

	s.touchLocked(entry, now, window)
	if metered {
		entry.rejections++
		s.advanceWindowLocked(key, entry, now, window)
	}
}

// Closes an elapsed window and applies at most one rate change. A window with too little
// evidence is closed without deciding and its counters carry forward.
func (s *ThrottleState) advanceWindowLocked(
	key ThrottleKey,
	entry *throttleEntry,
	now time.Time,
	window time.Duration,
) {
	if now.Sub(entry.windowStart) < window {
		return
	}
	// Credit the window at the rate that governed it, before the decision changes it.
	entry.refillLocked(now, window)
	entry.windowStart = now

	// Read once: the evidence gate and the comparison below must use the same threshold.
	lossThreshold := s.settings().LossThreshold
	// Below 1/threshold releases, one rejection would read as total loss.
	if float64(entry.releases)*lossThreshold < 1 {
		return
	}
	defer func() {
		entry.releases, entry.rejections, entry.suppressions = 0, 0, 0
	}()

	// Can exceed 1 when a rejection lands a window late; it is only compared to the threshold.
	loss := float64(entry.rejections) / float64(entry.releases)
	switch {
	case loss > lossThreshold:
		entry.rate = s.clamp(entry.rate * s.settings().Beta)
		// Tokens banked at the old rate would let the class overshoot the new one.
		entry.tokens = min(entry.tokens, entry.burstLocked(window))
	case entry.suppressions > 0:
		entry.rate = s.clamp(entry.rate * (1 + s.settings().IncreaseRatio))
	default:
		// Never refused, so it has not asked for more. Raising it would grow the burst.
		return
	}
	metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, key.metricsTags()...)
}

func (s *ThrottleState) touchLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if now.Sub(entry.lastAccess) > s.settings().KeyTTL {
		entry.rate = s.clamp(s.settings().InitialRate)
		entry.lastRefill = now
		entry.windowStart = now
		entry.releases, entry.rejections, entry.suppressions = 0, 0, 0
		entry.tokens = entry.burstLocked(window)
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

func (s *ThrottleState) clamp(rate float64) float64 {
	floor := max(s.settings().MinRate, minThrottleRate)
	return min(max(rate, floor), s.settings().MaxRate)
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
	if len(s.entries) >= s.settings().MaxKeys {
		return nil
	}

	entry := &throttleEntry{
		rate:        s.clamp(s.settings().InitialRate),
		lastRefill:  now,
		windowStart: now,
		lastAccess:  now,
	}
	entry.tokens = entry.burstLocked(s.settings().Window)
	s.entries[key] = entry
	metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(float64(len(s.entries)))
	return entry
}

func (s *ThrottleState) maybeSweepLocked(now time.Time) {
	if now.Sub(s.lastSweep) < s.settings().KeyTTL/throttleSweepDivisor {
		return
	}
	s.lastSweep = now
	for key, entry := range s.entries {
		entry.Lock()
		idle := now.Sub(entry.lastAccess) > s.settings().KeyTTL
		entry.Unlock()
		if idle {
			delete(s.entries, key)
		}
	}
	metrics.TaskThrottleKeysTracked.With(s.metricsHandler).Record(float64(len(s.entries)))
}
