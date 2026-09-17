package queues

import (
	"math"
	"sync"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
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
	ThrottleKey struct {
		Cause       enumspb.ResourceExhaustedCause
		NamespaceID string
	}

	ThrottleStateOptions struct {
		Enabled       dynamicconfig.BoolPropertyFn
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
		releases    int64
		rejections  int64
		pending     int64 // unresolved admits
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
		minRate:        defaultThrottleMinRate,
		maxRate:        defaultThrottleMaxRate,
		initialRate:    defaultThrottleInitialRate,
		keyTTL:         defaultThrottleKeyTTL,
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
	if s.options.Window != nil {
		if window := s.options.Window(); window > 0 {
			return window
		}
	}
	return defaultThrottleWindow
}

func (s *ThrottleState) Admit(key ThrottleKey) (allowed bool, permit *throttleEntry, retryAfter time.Duration) {
	if !s.Enabled() {
		return true, nil, 0
	}
	entry := s.getOrCreate(key)
	if entry == nil {
		metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.cappedTags()...)
		return true, nil, 0
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
		return false, nil, entry.tokenETALocked()
	}

	entry.tokens--
	entry.pending++
	metrics.TaskThrottleGateAdmitted.With(s.metricsHandler).Record(1, key.metricsTags()...)
	return true, entry, 0
}

func (s *ThrottleState) Finish(permit *throttleEntry, submitted bool) {
	if permit == nil {
		return
	}

	now := s.timeSource.Now()
	window := s.Window()
	permit.Lock()
	if submitted {
		permit.releases++
	} else {
		permit.tokens = min(permit.tokens+1, permit.burstLocked(window))
	}
	permit.pending--
	s.advanceWindowLocked(permit, now, window)
	permit.Unlock()
}

func (s *ThrottleState) ReportThrottled(key ThrottleKey, permit *throttleEntry) {
	if !s.Enabled() {
		return
	}
	admitted := permit != nil && permit.key == key
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
	if admitted {
		entry.rejections++
		s.advanceWindowLocked(entry, now, window)
	}
}

func (s *ThrottleState) advanceWindowLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if now.Sub(entry.windowStart) < window {
		return
	}
	entry.refillLocked(now, window)
	defer func() {
		entry.windowStart = now
		entry.releases, entry.rejections = 0, 0
	}()
	if entry.releases == 0 && entry.rejections == 0 {
		return
	}

	loss := 1.0
	if entry.rejections < entry.releases {
		loss = float64(entry.rejections) / float64(entry.releases)
	}
	beta, increaseRatio, lossThreshold := s.controlLaw()
	if loss > lossThreshold {
		entry.rate = s.clamp(entry.rate * beta)
		entry.tokens = min(entry.tokens, entry.burstLocked(window))
		metrics.TaskThrottleRateDecreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	} else {
		entry.rate = s.clamp(entry.rate * (1 + increaseRatio))
		metrics.TaskThrottleRateIncreases.With(s.metricsHandler).Record(1, entry.key.metricsTags()...)
	}
	metrics.TaskThrottleAdmittedRate.With(s.metricsHandler).Record(entry.rate, entry.key.metricsTags()...)
}

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

func (e *throttleEntry) resetLocked(rate float64, now time.Time, window time.Duration) {
	e.rate = rate
	e.lastRefill = now
	e.windowStart = now
	e.releases, e.rejections = 0, 0
	e.tokens = e.burstLocked(window)
}

func (s *ThrottleState) touchLocked(entry *throttleEntry, now time.Time, window time.Duration) {
	if !entry.lastAccess.IsZero() && now.Sub(entry.lastAccess) > s.keyTTL {
		entry.resetLocked(s.initialRate, now, window)
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
		return s.minRate
	}
	return min(max(rate, s.minRate), s.maxRate)
}

func (s *ThrottleState) controlLaw() (beta, increaseRatio, lossThreshold float64) {
	beta = defaultThrottleBeta
	if s.options.Beta != nil {
		if configured := s.options.Beta(); configured > 0 && configured < 1 {
			beta = configured
		}
	}

	increaseRatio = defaultThrottleIncreaseRatio
	if s.options.IncreaseRatio != nil {
		if configured := s.options.IncreaseRatio(); configured > 0 {
			increaseRatio = configured
		}
	}

	lossThreshold = defaultThrottleLossThreshold
	if s.options.LossThreshold != nil {
		if configured := s.options.LossThreshold(); configured >= 0 && configured < 1 {
			lossThreshold = configured
		}
	}
	return beta, increaseRatio, lossThreshold
}

func (s *ThrottleState) maxKeys() int {
	if s.options.MaxKeys != nil {
		if configured := s.options.MaxKeys(); configured > 0 {
			return configured
		}
	}
	return defaultThrottleMaxKeys
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
		rate:        s.initialRate,
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
	if now.Sub(s.lastSweep) < s.keyTTL/throttleSweepDivisor {
		return
	}
	s.lastSweep = now
	evicted := false
	for key, entry := range s.entries {
		entry.Lock()
		idle := entry.pending == 0 && now.Sub(entry.lastAccess) > s.keyTTL
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
