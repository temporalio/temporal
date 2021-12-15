// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

import (
	"fmt"
	"time"

	"github.com/uber-go/tally/v4"
)

var _ MetricTestUtility = (*TallyMetricTestUtility)(nil)

type (
	TallyMetricTestUtility struct {
		scope tally.TestScope
	}
)

func NewTallyMetricTestUtility() *TallyMetricTestUtility {
	return &TallyMetricTestUtility{
		scope: tally.NewTestScope("", nil),
	}
}

func (t *TallyMetricTestUtility) GetClient(config *ClientConfig, idx ServiceIdx) Client {
	return NewClient(config, t.scope, idx)
}

func (t *TallyMetricTestUtility) ContainsCounter(name MetricName, labels map[string]string, value int64) error {
	counters := t.scope.Snapshot().Counters()
	sname := string(name)
	key := tally.KeyForPrefixedStringMap(sname, labels)
	counter, ok := counters[key]
	if !ok {
		return fmt.Errorf("%s is missing in counters set", name)
	}
	if counter.Value() != value {
		return fmt.Errorf("%s value %d != expected %d", name, counter.Value(), value)
	}
	return nil
}

func (t *TallyMetricTestUtility) ContainsGauge(name MetricName, labels map[string]string, value float64) error {
	counters := t.scope.Snapshot().Gauges()
	sname := string(name)
	key := tally.KeyForPrefixedStringMap(sname, labels)
	counter, ok := counters[key]
	if !ok {
		return fmt.Errorf("%s is missing in counters set", name)
	}
	if counter.Value() != value {
		return fmt.Errorf("%s value %f != expected %f", name, counter.Value(), value)
	}
	return nil
}

func (t *TallyMetricTestUtility) ContainsTimer(name MetricName, labels map[string]string, value time.Duration) error {
	counters := t.scope.Snapshot().Timers()
	sname := string(name)
	key := tally.KeyForPrefixedStringMap(sname, labels)
	counter, ok := counters[key]
	if !ok {
		return fmt.Errorf("%s is missing in counters set", name)
	}
	if len(counter.Values()) > 1 {
		return fmt.Errorf("multiple values not supported")
	}
	if counter.Values()[0] != value {
		return fmt.Errorf("%s value %s != expected %s", name, counter.Values()[0], value)
	}
	return nil
}

// This function doesn't validate value. Only tags.
func (t *TallyMetricTestUtility) ContainsHistogram(name MetricName, labels map[string]string, value int) error {
	counters := t.scope.Snapshot().Histograms()
	sname := string(name)
	key := tally.KeyForPrefixedStringMap(sname, labels)
	_, ok := counters[key]
	if !ok {
		return fmt.Errorf("%s is missing in counters set", name)
	}
	// todo: tally doesn't have aggregation or exact value in test. So skipping value validation.
	return nil
}

func (t *TallyMetricTestUtility) CollectionSize() int {
	return len(t.scope.Snapshot().Counters()) +
		len(t.scope.Snapshot().Gauges()) +
		len(t.scope.Snapshot().Timers()) +
		len(t.scope.Snapshot().Histograms())

}
