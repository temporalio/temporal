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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/prometheus"

	"github.com/uber/cadence/common/log/loggerimpl"
)

type MetricsSuite struct {
	*require.Assertions
	suite.Suite
}

func TestMetricsSuite(t *testing.T) {
	suite.Run(t, new(MetricsSuite))
}

func (s *MetricsSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *MetricsSuite) TestStatsd() {
	statsd := &Statsd{
		HostPort: "127.0.0.1:8125",
		Prefix:   "testStatsd",
	}

	config := new(Metrics)
	config.Statsd = statsd
	scope := config.NewScope(loggerimpl.NewNopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestM3() {
	m3 := &m3.Configuration{
		HostPort: "127.0.0.1:8125",
		Service:  "testM3",
		Env:      "devel",
	}
	config := new(Metrics)
	config.M3 = m3
	scope := config.NewScope(loggerimpl.NewNopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestPrometheus() {
	prom := &prometheus.Configuration{
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
	}
	config := new(Metrics)
	config.Prometheus = prom
	scope := config.NewScope(loggerimpl.NewNopLogger())
	s.NotNil(scope)
}

func (s *MetricsSuite) TestNoop() {
	config := &Metrics{}
	scope := config.NewScope(loggerimpl.NewNopLogger())
	s.Equal(tally.NoopScope, scope)
}
