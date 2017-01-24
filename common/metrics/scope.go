package metrics

import "github.com/uber-go/tally"

type cachedMetricScope struct {
	tally.Scope
	counters map[string]tally.Counter
	timers   map[string]tally.Timer
	gauges   map[string]tally.Gauge
}

func newScope(scope tally.Scope, metricDefs map[MetricName]MetricType) tally.Scope {
	s := &cachedMetricScope{
		Scope:    scope,
		counters: make(map[string]tally.Counter),
		timers:   make(map[string]tally.Timer),
		gauges:   make(map[string]tally.Gauge),
	}

	for name, t := range metricDefs {
		switch t {
		case Counter:
			s.counters[string(name)] = s.Scope.Counter(string(name))
		case Timer:
			s.timers[string(name)] = s.Scope.Timer(string(name))
		case Gauge:
			s.gauges[string(name)] = s.Scope.Gauge(string(name))
		}
	}

	return s
}

func (s *cachedMetricScope) Counter(name string) tally.Counter {
	counter, ok := s.counters[name]
	if !ok {
		// this is not a cached metric. Fall back to tally
		counter = s.Scope.Counter(name)
	}
	return counter
}

func (s *cachedMetricScope) Timer(name string) tally.Timer {
	timer, ok := s.timers[name]
	if !ok {
		// this is not a cached metric. Fall back to tally
		timer = s.Scope.Timer(name)
	}
	return timer
}

func (s *cachedMetricScope) Gauge(name string) tally.Gauge {
	gauge, ok := s.gauges[name]
	if !ok {
		// this is not a cached metric. Fall back to tally
		gauge = s.Scope.Gauge(name)
	}
	return gauge
}
