package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/prometheus"

	"github.com/temporalio/temporal/common/log/loggerimpl"
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
