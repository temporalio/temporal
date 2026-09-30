package persistence

import (
	"time"
)

var NoopHealthSignalAggregator HealthSignalAggregator = newNoopSignalAggregator()

type (
	noopSignalAggregator struct{}
)

func newNoopSignalAggregator() *noopSignalAggregator { return &noopSignalAggregator{} }

func (a *noopSignalAggregator) Start() {}

func (a *noopSignalAggregator) Stop() {}

func (a *noopSignalAggregator) Record(_ int32, _ time.Duration, _ error) {}

func (a *noopSignalAggregator) AverageLatency() float64 {
	return 0
}

func (a *noopSignalAggregator) LatencyQuantile(_ float64) (float64, bool) {
	return 0, false
}

func (a *noopSignalAggregator) LatencyQuantileByGroup(_ string, _ float64) (float64, bool) {
	return 0, false
}

func (*noopSignalAggregator) ErrorRatio() (float64, bool) {
	return 0, false
}

func (a *noopSignalAggregator) ErrorRatioByGroup(_ string) (float64, bool) {
	return 0, false
}
