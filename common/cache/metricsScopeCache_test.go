// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/uber/cadence/common/metrics"
)

func TestGetMetricsScope(t *testing.T) {
	metricsCache := NewDomainMetricsScopeCache()
	var found bool

	tests := []struct {
		scopeID  int
		domainID string
	}{
		{1, "A"},
		{2, "B"},
		{1, "C"},
	}

	for _, t := range tests {
		mockMetricsScope := metrics.NoopScope(metrics.ServiceIdx(t.scopeID))
		metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	metricsScope, found := metricsCache.Get("A", 1)
	testMetricsScope := metrics.NoopScope(metrics.ServiceIdx(1))
	assert.Equal(t, testMetricsScope, metricsScope)
	assert.Equal(t, found, true)

	_, found = metricsCache.Get("B", 2)
	assert.Equal(t, found, true)

	metricsScope, found = metricsCache.Get("C", 1)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	assert.NotEqual(t, testMetricsScope, metricsScope)
	assert.Equal(t, found, true)

	metricsScope, found = metricsCache.Get("D", 3)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	assert.NotEqual(t, testMetricsScope, metricsScope)
	assert.Equal(t, found, false)
}

func TestConcurrentMetricsScopeAccess(t *testing.T) {
	domainMetricsScopeCache := NewDomainMetricsScopeCache()

	ch := make(chan struct{})
	var wg sync.WaitGroup
	var metricsScope, testMetricsScope metrics.Scope
	var found bool

	for i := 0; i < 1000; i++ {
		wg.Add(1)
		// concurrent get and put
		go func(scopeIdx int) {
			defer wg.Done()

			<-ch

			domainMetricsScopeCache.Get("test_domain", scopeIdx)
			domainMetricsScopeCache.Put("test_domain", scopeIdx, metrics.NoopScope(metrics.ServiceIdx(scopeIdx)))
		}(i)
	}

	close(ch)
	wg.Wait()

	for i := 0; i < 1000; i++ {
		metricsScope, found = domainMetricsScopeCache.Get("test_domain", i)
		testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(i))

		assert.Equal(t, true, found)
		assert.Equal(t, testMetricsScope, metricsScope)
	}
}
