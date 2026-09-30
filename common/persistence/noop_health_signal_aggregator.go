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

func (*noopSignalAggregator) ErrorRatio() float64 {
	return 0
}
