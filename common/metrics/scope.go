// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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
