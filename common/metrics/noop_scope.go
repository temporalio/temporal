package metrics

import (
	"time"
)

type (
	NoopScopeImpl struct{}
)

func (n NoopScopeImpl) AddCounterInternal(name string, delta int64) {
}

func (n NoopScopeImpl) StartTimerInternal(timer string) Stopwatch {
	return nopStopwatch{}
}

func (n NoopScopeImpl) RecordTimerInternal(timer string, d time.Duration) {
}

func (n NoopScopeImpl) RecordDistributionInternal(id string, d int) {
}

func (n NoopScopeImpl) TaggedInternal(tags ...Tag) internalScope {
	return n
}

// NoopScope returns a noop scope of metrics
func NoopScope(serviceIdx ServiceIdx) internalScope {
	return &NoopScopeImpl{}
}

func (n NoopScopeImpl) IncCounter(counter int) {}

func (n NoopScopeImpl) AddCounter(counter int, delta int64) {}

func (n NoopScopeImpl) StartTimer(timer int) Stopwatch {
	return nopStopwatch{}
}

func (n NoopScopeImpl) RecordTimer(timer int, d time.Duration) {}

func (n NoopScopeImpl) RecordDistribution(id int, d int) {}

func (n NoopScopeImpl) UpdateGauge(gauge int, value float64) {}

func (n NoopScopeImpl) Tagged(tags ...Tag) Scope {
	return n
}
