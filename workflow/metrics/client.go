package metrics

import (
	"time"

	"code.uber.internal/devexp/minions/common"
)

// ClientImpl is for m3 emits within inputhost
type ClientImpl struct {
	//parentReporter is the parent for the metrics Reporters
	parentReporter common.Reporter
	// childReporters is the children for the metrics Reporters
	childReporters []common.Reporter

	// timerName is the map of all TimerName for  metrics
	timerName map[int]string

	// counterName is the map of all CounterName for  metrics
	counterName map[int]string

	// gaugeName is the map of all GaugeName for  metrics
	gaugeName map[int]string
}

// NewClient creates and returns a new instance of
// Client implementation
// reporter holds the common tags for the servcie
// serviceIdx indicates the service type in (InputhostIndex, ... StorageIndex)
func NewClient(reporter common.Reporter, serviceIdx int) common.Client {
	counterName := CounterNames[serviceIdx]
	timerName := TimerNames[serviceIdx]
	gaugeName := GaugeNames[serviceIdx]
	scopeTagsMap := ScopeToTags[serviceIdx]
	size := len(counterName) + len(timerName) + len(gaugeName)
	metricsMap := make(map[common.MetricName]common.MetricType, size)
	for _, val := range counterName {
		metricsMap[common.MetricName(val)] = Counter
	}
	for _, val := range timerName {
		metricsMap[common.MetricName(val)] = Timer
	}
	for _, val := range gaugeName {
		metricsMap[common.MetricName(val)] = Gauge
	}
	metricsClient := &ClientImpl{
		parentReporter: reporter,
		counterName:    counterName,
		timerName:      timerName,
		gaugeName:      gaugeName,
	}
	metricsClient.childReporters = make([]common.Reporter, len(scopeTagsMap))
	for i := 0; i < len(scopeTagsMap); i++ {
		metricsClient.childReporters[i] = reporter.GetChildReporter(scopeTagsMap[i])
		metricsClient.childReporters[i].InitMetrics(metricsMap)
	}
	return metricsClient
}

// IncCounter increments one for a counter and emits
// to m3 backend
func (m *ClientImpl) IncCounter(scopeIdx int, counterIdx int) {
	m.childReporters[scopeIdx].IncCounter(m.counterName[counterIdx], nil, 1)
}

// AddCounter adds delta to the counter and
// emits to the m3 backend
func (m *ClientImpl) AddCounter(scopeIdx int, counterIdx int, delta int64) {
	m.childReporters[scopeIdx].IncCounter(m.counterName[counterIdx], nil, delta)
}

// StartTimer starts a timer for the given
// metric name
func (m *ClientImpl) StartTimer(scopeIdx int, timerIdx int) common.Stopwatch {
	return m.childReporters[scopeIdx].StartTimer(m.timerName[timerIdx], nil)
}

// RecordTimer record and emit a timer for the given
// metric name
func (m *ClientImpl) RecordTimer(scopeIdx int, timerIdx int, d time.Duration) {
	m.childReporters[scopeIdx].RecordTimer(m.timerName[timerIdx], nil, d)
}

// UpdateGauge reports Gauge type metric to M3
func (m *ClientImpl) UpdateGauge(scopeIdx int, gaugeIdx int, delta int64) {
	m.childReporters[scopeIdx].UpdateGauge(m.gaugeName[gaugeIdx], nil, delta)
}

// GetParentReporter return the parentReporter
func (m *ClientImpl) GetParentReporter() common.Reporter {
	return m.parentReporter
}
