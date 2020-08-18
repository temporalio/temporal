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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/common/metrics"
)

type domainMetricsCacheSuite struct {
	suite.Suite
	*require.Assertions

	metricsCache DomainMetricsScopeCache
}

func TestDomainMetricsCacheSuite(t *testing.T) {
	s := new(domainMetricsCacheSuite)
	suite.Run(t, s)
}

func (s *domainMetricsCacheSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	metricsCache := NewDomainMetricsScopeCache().(*domainMetricsScopeCache)
	metricsCache.flushDuration = 100 * time.Millisecond
	s.metricsCache = metricsCache

	s.metricsCache.Start()
}

func (s *domainMetricsCacheSuite) TearDownTest() {
	s.metricsCache.Stop()
}

func (s *domainMetricsCacheSuite) TestGetMetricsScope() {
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
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	time.Sleep(110 * time.Millisecond)

	metricsScope, found := s.metricsCache.Get("A", 1)
	testMetricsScope := metrics.NoopScope(metrics.ServiceIdx(1))
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	_, found = s.metricsCache.Get("B", 2)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("C", 1)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("D", 3)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)
}

func (s *domainMetricsCacheSuite) TestGetMetricsScopeMultipleFlushLoop() {
	var found bool

	tests := []struct {
		scopeID  int
		domainID string
	}{
		{1, "A"},
		{2, "B"},
		{1, "C"},
		{5, "D"},
		{3, "E"},
	}

	for i := 0; i < 3; i++ {
		t := tests[i]
		mockMetricsScope := metrics.NoopScope(metrics.ServiceIdx(t.scopeID))
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	time.Sleep(110 * time.Millisecond)

	for i := 3; i < len(tests); i++ {
		t := tests[i]
		mockMetricsScope := metrics.NoopScope(metrics.ServiceIdx(t.scopeID))
		s.metricsCache.Put(t.domainID, t.scopeID, mockMetricsScope)
	}

	metricsScope, found := s.metricsCache.Get("A", 1)
	testMetricsScope := metrics.NoopScope(metrics.ServiceIdx(1))
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	_, found = s.metricsCache.Get("B", 2)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("C", 1)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("D", 5)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(5))
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)

	metricsScope, found = s.metricsCache.Get("E", 3)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	s.NotEqual(testMetricsScope, metricsScope)
	s.Equal(found, false)

	time.Sleep(200 * time.Millisecond)

	metricsScope, found = s.metricsCache.Get("D", 5)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(5))
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)

	metricsScope, found = s.metricsCache.Get("E", 3)
	testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(3))
	s.Equal(testMetricsScope, metricsScope)
	s.Equal(found, true)
}

func (s *domainMetricsCacheSuite) TestConcurrentMetricsScopeAccess() {

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

			s.metricsCache.Get("test_domain", scopeIdx)
			s.metricsCache.Put("test_domain", scopeIdx, metrics.NoopScope(metrics.ServiceIdx(scopeIdx)))
		}(i)
	}

	close(ch)
	wg.Wait()

	time.Sleep(120 * time.Millisecond)

	for i := 0; i < 1000; i++ {
		metricsScope, found = s.metricsCache.Get("test_domain", i)
		testMetricsScope = metrics.NoopScope(metrics.ServiceIdx(i))

		s.Equal(true, found)
		s.Equal(testMetricsScope, metricsScope)
	}
}
