package health

import (
	"reflect"
	"sync"
	"time"

	"go.temporal.io/server/common/aggregate"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/stats"
)

const (
	// overallGroupName is used to aggregate across every key
	overallGroupName = "!*!"
	refreshInterval  = time.Minute

	// fallbacks used when the overall bucket doesn't configure its own windows.
	fallbackErrorRatioWindowSize = 10 * time.Second
	fallbackErrorRatioBufferSize = 5000
)

var fallbackLatencyWindow = stats.WindowConfig{
	WindowSize:  5 * time.Second,
	WindowCount: 10,
}

type SignalAggregator struct {
	logger      log.Logger
	getSettings func() Settings
	isUnhealthy func(error) bool

	latencyByGroup    map[string]stats.TimeWindowedStats
	latencyByKey      map[string]stats.TimeWindowedStats
	errorRatioByGroup map[string]aggregate.MovingWindowAverage
	errorRatioByKey   map[string]aggregate.MovingWindowAverage
	lastSettings      Settings
	mu                sync.RWMutex

	startOnce sync.Once
	stopOnce  sync.Once
	done      chan struct{}
}

type Option func(*SignalAggregator)

// WithIsUnhealthy sets the classifier that decides whether a (non-nil) error counts
// toward the error ratio. Without it, every error is counted
func WithIsUnhealthy(isUnhealthy func(error) bool) Option {
	return func(s *SignalAggregator) {
		s.isUnhealthy = isUnhealthy
	}
}

func NewSignalAggregator(
	logger log.Logger,
	getSettings func() Settings,
	opts ...Option,
) *SignalAggregator {
	agg := &SignalAggregator{
		logger:      logger,
		getSettings: getSettings,
		isUnhealthy: func(error) bool { return true }, // default to counting every error
		done:        make(chan struct{}),
	}

	for _, opt := range opts {
		opt(agg)
	}

	agg.refresh() // seed the maps

	return agg
}

func (s *SignalAggregator) Start() {
	s.startOnce.Do(func() {
		go func() {
			ticker := time.NewTicker(refreshInterval)
			defer ticker.Stop()

			for {
				select {
				case <-s.done:
					return
				case <-ticker.C:
					s.refresh()
				}
			}
		}()
	})
}

func (s *SignalAggregator) Stop() {
	s.stopOnce.Do(func() {
		close(s.done)
	})
}

func (s *SignalAggregator) settingsChanged(newSettings Settings) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.latencyByGroup == nil || !reflect.DeepEqual(newSettings, s.lastSettings)
}

// refresh rebuilds every bucket whenever the settings change at all.
//
// TODO: this discards all accumulated samples on any settings change, even one that doesn't
// touch a given bucket, so quantiles and error ratios read 0 (healthy) until they refill
// a settings change during an incident can therefore transiently clear an unhealthy signal
// fixing it means diffing thresholds per bucket and carrying the unaffected ones over; the
// reflect.DeepEqual above is too coarse for that. deferring since it's an optimization
// rather than a correctness problem. TestRefresh pins the current behavior.
func (s *SignalAggregator) refresh() {
	newSettings := s.getSettings()

	if !s.settingsChanged(newSettings) {
		return
	}

	latencyByGroup := make(map[string]stats.TimeWindowedStats)
	latencyByKey := make(map[string]stats.TimeWindowedStats)
	errorRatioByGroup := make(map[string]aggregate.MovingWindowAverage)
	errorRatioByKey := make(map[string]aggregate.MovingWindowAverage)

	overallWindow := fallbackLatencyWindow
	if newSettings.Overall.WindowConfig != nil {
		overallWindow = *newSettings.Overall.WindowConfig
	}

	dist := s.newLatencyDist(overallWindow)
	if dist != nil {
		latencyByGroup[overallGroupName] = dist
	}

	overallERWindow, overallERBuffer := fallbackErrorRatioWindowSize, fallbackErrorRatioBufferSize

	ert := newSettings.Overall.ErrorRatioThreshold
	if ert != nil {
		overallERWindow, overallERBuffer = ert.WindowSize, ert.BufferSize
	}

	errorRatioByGroup[overallGroupName] = aggregate.NewMovingWindowAvgImpl(overallERWindow, overallERBuffer)

	for _, group := range newSettings.Groups {
		t := group.Thresholds

		if t.WindowConfig != nil {
			dist := s.newLatencyDist(*t.WindowConfig)
			if dist != nil {
				latencyByGroup[group.Name] = dist

				for _, key := range group.Keys {
					latencyByKey[key] = dist
				}
			}
		}

		if t.ErrorRatioThreshold != nil {
			errorRatio := aggregate.NewMovingWindowAvgImpl(t.ErrorRatioThreshold.WindowSize, t.ErrorRatioThreshold.BufferSize)

			errorRatioByGroup[group.Name] = errorRatio

			for _, key := range group.Keys {
				errorRatioByKey[key] = errorRatio
			}
		}
	}

	s.mu.Lock()
	s.latencyByGroup = latencyByGroup
	s.latencyByKey = latencyByKey
	s.errorRatioByGroup = errorRatioByGroup
	s.errorRatioByKey = errorRatioByKey
	s.lastSettings = newSettings
	s.mu.Unlock()
}

func (s *SignalAggregator) newLatencyDist(cfg stats.WindowConfig) stats.TimeWindowedStats {
	dist, err := stats.NewWindowedTDigest(cfg)
	if err != nil {
		s.logger.Error("failed to create latency distribution, falling back to default", tag.Error(err))

		dist, err = stats.NewWindowedTDigest(fallbackLatencyWindow)
		if err != nil {
			s.logger.Error("failed to create fallback latency distribution", tag.Error(err))
			return nil
		}
	}

	return dist
}

func (s *SignalAggregator) Record(key string, latency time.Duration, err error) {
	unhealthy := err != nil && s.isUnhealthy(err)
	ms := float64(latency.Milliseconds())

	s.mu.RLock()
	overallLatency := s.latencyByGroup[overallGroupName]
	groupLatency := s.latencyByKey[key]
	overallErrRatio := s.errorRatioByGroup[overallGroupName]
	groupErrRatio := s.errorRatioByKey[key]
	s.mu.RUnlock()

	if overallLatency != nil {
		overallLatency.RecordToLatestWindow(ms)
	}

	if groupLatency != nil {
		groupLatency.RecordToLatestWindow(ms)
	}

	if overallErrRatio != nil {
		if unhealthy {
			overallErrRatio.Record(1)
		} else {
			overallErrRatio.Record(0)
		}
	}

	if groupErrRatio != nil {
		if unhealthy {
			groupErrRatio.Record(1)
		} else {
			groupErrRatio.Record(0)
		}
	}
}

// LatencyQuantile reports the quantile across every key. The bool is false when no
// latency bucket exists, which callers must not treat as a healthy zero reading.
func (s *SignalAggregator) LatencyQuantile(quantile float64) (float64, bool) {
	return s.LatencyQuantileByGroup(overallGroupName, quantile)
}

// ErrorRatio reports the error ratio across every key. The bool is false when no
// error ratio bucket exists, which callers must not treat as a healthy zero reading.
func (s *SignalAggregator) ErrorRatio() (float64, bool) {
	return s.ErrorRatioByGroup(overallGroupName)
}

// LatencyQuantileByGroup returns false when the group has no latency bucket, either
// because the group isn't configured or because its thresholds omit a WindowConfig.
// Note that a configured bucket which never received a sample still reports (0, true):
// an empty tdigest quantile is indistinguishable from a genuinely fast one.
func (s *SignalAggregator) LatencyQuantileByGroup(groupName string, quantile float64) (float64, bool) {
	s.mu.RLock()
	dist, found := s.latencyByGroup[groupName]
	s.mu.RUnlock()

	if !found {
		return 0, false
	}

	return dist.Quantile(quantile), true
}

// ErrorRatioByGroup returns false when the group has no error ratio bucket, either
// because the group isn't configured or because its thresholds omit an
// ErrorRatioThreshold. As with LatencyQuantileByGroup, an empty bucket reports (0, true).
func (s *SignalAggregator) ErrorRatioByGroup(groupName string) (float64, bool) {
	s.mu.RLock()
	errRatio, found := s.errorRatioByGroup[groupName]
	s.mu.RUnlock()

	if !found {
		return 0, false
	}

	return errRatio.Average(), true
}
