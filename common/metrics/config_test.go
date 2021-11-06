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
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/m3"

	"go.temporal.io/server/common/log"
)

type nullStatsReporter struct{}
type unsupportedNullStatsReporter struct{}

var CachedNullStatsReporter tally.CachedStatsReporter = nullStatsReporter{}
var UnsupportedNullStatsReporter tally.BaseStatsReporter = unsupportedNullStatsReporter{}

func (r nullStatsReporter) Capabilities() tally.Capabilities {
	panic("implement me")
}

func (r nullStatsReporter) Flush() {
	panic("implement me")
}

func (r nullStatsReporter) AllocateCounter(name string, tags map[string]string) tally.CachedCount {
	panic("implement me")
}

func (r nullStatsReporter) AllocateGauge(name string, tags map[string]string) tally.CachedGauge {
	panic("implement me")
}

func (r nullStatsReporter) AllocateTimer(name string, tags map[string]string) tally.CachedTimer {
	panic("implement me")
}

func (r nullStatsReporter) AllocateHistogram(name string, tags map[string]string, buckets tally.Buckets) tally.CachedHistogram {
	panic("implement me")
}

func (u unsupportedNullStatsReporter) Capabilities() tally.Capabilities {
	panic("implement me")
}

func (u unsupportedNullStatsReporter) Flush() {
	panic("implement me")
}

type MetricsSuite struct {
	*require.Assertions
	suite.Suite
	controller *gomock.Controller
}

func TestMetricsSuite(t *testing.T) {
	suite.Run(t, new(MetricsSuite))
}

func (s *MetricsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())
}

func (s *MetricsSuite) TestStatsd() {
	statsd := &StatsdConfig{
		HostPort: "127.0.0.1:8125",
		Prefix:   "testStatsd",
	}

	config := new(Config)
	config.Statsd = statsd
	scope := config.NewScope(log.NewNoopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestM3() {
	m3 := &m3.Configuration{
		HostPort: "127.0.0.1:8125",
		Service:  "testM3",
		Env:      "devel",
	}
	config := new(Config)
	config.M3 = m3
	scope := config.NewScope(log.NewNoopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestPrometheus() {
	prom := &PrometheusConfig{
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
	}
	config := new(Config)
	config.Prometheus = prom
	scope := config.NewScope(log.NewNoopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestNoop() {
	config := &Config{}
	scope := config.NewScope(log.NewNoopLogger())
	s.Equal(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestCustomReporter() {
	config := &Config{}
	scope := config.NewCustomReporterScope(log.NewNoopLogger(), tally.NullStatsReporter)
	s.NotNil(scope)
	s.NotEqual(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestCustomCachedReporter() {
	config := &Config{}
	scope := config.NewCustomReporterScope(log.NewNoopLogger(), CachedNullStatsReporter)
	s.NotNil(scope)
	s.NotEqual(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestUnsupportedReporter() {
	config := &Config{}
	scope := config.NewCustomReporterScope(log.NewNoopLogger(), UnsupportedNullStatsReporter)
	s.Equal(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestOTCustomReporter() {
	prom := &PrometheusConfig{
		Framework:     "custom",
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
	}
	config := &Config{}
	config.Prometheus = prom
	mockReporter := NewMockReporter(s.controller)
	reporter, sdkReporter, err := config.InitMetricReporters(log.NewNoopLogger(), mockReporter)
	s.Equal(mockReporter, reporter)
	s.Equal(mockReporter, sdkReporter)
	s.Nil(err)
}
