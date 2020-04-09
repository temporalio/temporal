package metrics

import (
	"time"

	"github.com/uber-go/tally"
)

type metricsScope struct {
	scope             tally.Scope
	defs              map[int]metricDefinition
	isNamespaceTagged bool
}

func newMetricsScope(scope tally.Scope, defs map[int]metricDefinition, isNamespace bool) Scope {
	return &metricsScope{scope, defs, isNamespace}
}

// NoopScope returns a noop scope of metrics
func NoopScope(serviceIdx ServiceIdx) Scope {
	return &metricsScope{tally.NoopScope, getMetricDefs(serviceIdx), false}
}

func (m *metricsScope) IncCounter(id int) {
	name := string(m.defs[id].metricName)
	m.scope.Counter(name).Inc(1)
}

func (m *metricsScope) AddCounter(id int, delta int64) {
	name := string(m.defs[id].metricName)
	m.scope.Counter(name).Inc(delta)
}

func (m *metricsScope) UpdateGauge(id int, value float64) {
	name := string(m.defs[id].metricName)
	m.scope.Gauge(name).Update(value)
}

func (m *metricsScope) StartTimer(id int) Stopwatch {
	name := string(m.defs[id].metricName)
	timer := m.scope.Timer(name)
	if m.isNamespaceTagged {
		timerAll := m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(name)
		return NewStopwatch(timer, timerAll)
	}
	return NewStopwatch(timer)
}

func (m *metricsScope) RecordTimer(id int, d time.Duration) {
	name := string(m.defs[id].metricName)
	m.scope.Timer(name).Record(d)
	if m.isNamespaceTagged {
		m.scope.Tagged(map[string]string{namespace: namespaceAllValue}).Timer(name).Record(d)
	}
}

func (m *metricsScope) RecordHistogramDuration(id int, value time.Duration) {
	name := string(m.defs[id].metricName)
	m.scope.Histogram(name, m.getBuckets(id)).RecordDuration(value)
}

func (m *metricsScope) RecordHistogramValue(id int, value float64) {
	name := string(m.defs[id].metricName)
	m.scope.Histogram(name, m.getBuckets(id)).RecordValue(value)
}

func (m *metricsScope) Tagged(tags ...Tag) Scope {
	namespaceTagged := false
	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		if isNamespaceTagged(tag) {
			namespaceTagged = true
		}
		tagMap[tag.Key()] = tag.Value()
	}
	return newMetricsScope(m.scope.Tagged(tagMap), m.defs, namespaceTagged)
}

func (m *metricsScope) getBuckets(id int) tally.Buckets {
	if m.defs[id].buckets != nil {
		return m.defs[id].buckets
	}
	return tally.DefaultBuckets
}

func isNamespaceTagged(tag Tag) bool {
	return tag.Key() == namespace && tag.Value() != namespaceAllValue
}
