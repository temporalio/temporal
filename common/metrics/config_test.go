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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"github.com/uber-go/tally/v4/m3"

	"go.temporal.io/server/common/log"
)

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
	scope := NewScope(log.NewNoopLogger(), config)
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
	scope := NewScope(log.NewNoopLogger(), config)
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
	scope := NewScope(log.NewNoopLogger(), config)
	s.NotNil(scope)
}

func (s *MetricsSuite) TestPrometheusWithSanitizeOptions() {
	validChars := &ValidCharacters{
		Ranges: []SanitizeRange{
			{
				StartRange: "a",
				EndRange:   "z",
			},
			{
				StartRange: "A",
				EndRange:   "Z",
			},
			{
				StartRange: "0",
				EndRange:   "9",
			},
		},
		SafeCharacters: "-",
	}

	prom := &PrometheusConfig{
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
		SanitizeOptions: &SanitizeOptions{
			NameCharacters:       validChars,
			KeyCharacters:        validChars,
			ValueCharacters:      validChars,
			ReplacementCharacter: "_",
		},
	}
	config := new(Config)
	config.Prometheus = prom
	scope := NewScope(log.NewNoopLogger(), config)
	s.NotNil(scope)
}

func (s *MetricsSuite) TestNoop() {
	config := &Config{}
	scope := NewScope(log.NewNoopLogger(), config)
	s.Equal(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestSetDefaultPerUnitHistogramBoundaries() {
	type histogramTest struct {
		input        map[string][]float64
		expectResult map[string][]float64
	}

	customizedBoundaries := map[string][]float64{
		Dimensionless: {1},
		Milliseconds:  defaultPerUnitHistogramBoundaries[Milliseconds],
		Bytes:         defaultPerUnitHistogramBoundaries[Bytes],
	}
	testCases := []histogramTest{
		{
			input:        nil,
			expectResult: defaultPerUnitHistogramBoundaries,
		},
		{
			input: map[string][]float64{
				UnitNameDimensionless: {1},
				"notDefine":           {1},
			},
			expectResult: customizedBoundaries,
		},
	}

	for _, test := range testCases {
		config := &ClientConfig{PerUnitHistogramBoundaries: test.input}
		setDefaultPerUnitHistogramBoundaries(config)
		s.Equal(test.expectResult, config.PerUnitHistogramBoundaries)
	}
}

func TestMetricsHandlerFromConfig(t *testing.T) {
	t.Parallel()

	logger := log.NewTestLogger()

	for _, c := range []struct {
		name         string
		cfg          *Config
		expectedType interface{}
	}{
		{
			name:         "nil config",
			cfg:          nil,
			expectedType: &noopMetricsHandler{},
		},
		{
			name: "tally",
			cfg: &Config{
				Prometheus: &PrometheusConfig{
					Framework:     FrameworkTally,
					ListenAddress: "localhost:0",
				},
			},
			expectedType: &tallyMetricsHandler{},
		},
		{
			name: "opentelemetry",
			cfg: &Config{
				Prometheus: &PrometheusConfig{
					Framework:     FrameworkOpentelemetry,
					ListenAddress: "localhost:0",
				},
			},
			expectedType: &otelMetricsHandler{},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			handler, err := MetricsHandlerFromConfig(logger, c.cfg)
			require.NoError(t, err)
			t.Cleanup(func() {
				handler.Stop(logger)
			})
			assert.IsType(t, c.expectedType, handler)
		})
	}

}
