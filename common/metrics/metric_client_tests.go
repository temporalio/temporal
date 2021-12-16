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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type (
	MetricTestUtility interface {
		GetClient(config *ClientConfig, scopeIdx ServiceIdx) Client
		ContainsCounter(name MetricName, labels map[string]string, value int64) error
		ContainsGauge(name MetricName, labels map[string]string, value float64) error
		ContainsTimer(name MetricName, labels map[string]string, value time.Duration) error
		ContainsHistogram(name MetricName, labels map[string]string, value int) error
		CollectionSize() int
	}

	CounterMetadata struct {
		name       string
		labels     map[string]string
		value      int64
		floatValue float64
	}

	MetricTestSuiteBase struct {
		suite.Suite
		MetricTestUtilityProvider func() MetricTestUtility
		metricTestUtility         MetricTestUtility
		testClient                Client
	}
)

var defaultConfig = ClientConfig{
	Tags:                       nil,
	ExcludeTags:                map[string][]string{},
	Prefix:                     "",
	PerUnitHistogramBoundaries: map[string][]float64{Dimensionless: {0, 10, 100}, Bytes: {1024, 2048}},
}

func (s *MetricTestSuiteBase) SetupTest() {
	s.metricTestUtility = s.MetricTestUtilityProvider()
	s.testClient = s.metricTestUtility.GetClient(&defaultConfig, UnitTestService)
}

func (s *MetricTestSuiteBase) TestClientReportCounter() {
	s.testClient.AddCounter(
		TestScope1,
		TestCounterMetric1, 66)
	testDef := MetricDefs[UnitTestService][TestCounterMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsCounter(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, 66))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestClientReportGauge() {
	s.testClient.UpdateGauge(
		TestScope1,
		TestGaugeMetric1, 66)
	testDef := MetricDefs[UnitTestService][TestGaugeMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsGauge(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, 66))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestClientReportTimer() {
	targetDuration := time.Second * 100
	s.testClient.RecordTimer(
		TestScope1,
		TestTimerMetric1, targetDuration)
	testDef := MetricDefs[UnitTestService][TestTimerMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsTimer(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, targetDuration))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestClientReportHistogram() {
	s.testClient.RecordDistribution(
		TestScope1,
		TestDimensionlessHistogramMetric1, 66)
	testDef := MetricDefs[UnitTestService][TestDimensionlessHistogramMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsHistogram(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, 66))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestScopeReportCounter() {
	s.testClient.Scope(TestScope1).AddCounter(TestCounterMetric1, 66)
	testDef := MetricDefs[UnitTestService][TestCounterMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsCounter(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, 66))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestScopeReportGauge() {
	s.testClient.Scope(TestScope1).UpdateGauge(TestGaugeMetric1, 66)
	testDef := MetricDefs[UnitTestService][TestGaugeMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsGauge(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, 66))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}

func (s *MetricTestSuiteBase) TestScopeReportTimer() {
	targetDuration := time.Second * 100
	s.testClient.Scope(TestScope1).RecordTimer(TestTimerMetric1, targetDuration)
	testDef := MetricDefs[UnitTestService][TestTimerMetric1]
	assert.NoError(s.T(), s.metricTestUtility.ContainsTimer(testDef.metricName, map[string]string{namespace: namespaceAllValue, OperationTagName: ScopeDefs[UnitTestService][TestScope1].operation}, targetDuration))
	assert.Equal(s.T(), 1, s.metricTestUtility.CollectionSize())
}
