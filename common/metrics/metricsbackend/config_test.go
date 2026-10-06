package metricsbackend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
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
	statsd := &metrics.StatsdConfig{
		HostPort: "127.0.0.1:8125",
		Prefix:   "testStatsd",
	}

	config := new(metrics.Config)
	config.Statsd = statsd
	scope := NewScope(log.NewNoopLogger(), config)
	s.NotNil(scope)
}

func (s *MetricsSuite) TestPrometheus() {
	prom := &metrics.PrometheusConfig{
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
	}
	config := new(metrics.Config)
	config.Prometheus = prom
	scope := NewScope(log.NewNoopLogger(), config)
	s.NotNil(scope)
}

func (s *MetricsSuite) TestPrometheusWithSanitizeOptions() {
	validChars := &metrics.ValidCharacters{
		Ranges: []metrics.SanitizeRange{
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

	prom := &metrics.PrometheusConfig{
		OnError:       "panic",
		TimerType:     "histogram",
		ListenAddress: "127.0.0.1:0",
		SanitizeOptions: &metrics.SanitizeOptions{
			NameCharacters:       validChars,
			KeyCharacters:        validChars,
			ValueCharacters:      validChars,
			ReplacementCharacter: "_",
		},
	}
	config := new(metrics.Config)
	config.Prometheus = prom
	scope := NewScope(log.NewNoopLogger(), config)
	s.NotNil(scope)
}

func (s *MetricsSuite) TestNoop() {
	config := &metrics.Config{}
	scope := NewScope(log.NewNoopLogger(), config)
	s.Equal(tally.NoopScope, scope)
}

func (s *MetricsSuite) TestSetDefaultPerUnitHistogramBoundaries() {
	type histogramTest struct {
		input        map[string][]float64
		expectResult map[string][]float64
	}

	testCases := []histogramTest{
		{
			input: nil,
			expectResult: map[string][]float64{
				metrics.Dimensionless: defaultPerUnitHistogramBoundaries[metrics.Dimensionless],
				metrics.Milliseconds:  defaultPerUnitHistogramBoundaries[metrics.Milliseconds],
				metrics.Seconds:       {0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000},
				metrics.Bytes:         defaultPerUnitHistogramBoundaries[metrics.Bytes],
			},
		},
		{
			input: map[string][]float64{
				metrics.UnitNameDimensionless: {1},
				metrics.UnitNameMilliseconds:  {10, 1000, 2000},
				"notDefine":                   {1},
			},
			expectResult: map[string][]float64{
				metrics.Dimensionless: {1},
				metrics.Milliseconds:  {10, 1000, 2000},
				metrics.Seconds:       {0.01, 1, 2},
				metrics.Bytes:         defaultPerUnitHistogramBoundaries[metrics.Bytes],
			},
		},
	}

	for _, test := range testCases {
		config := &metrics.ClientConfig{PerUnitHistogramBoundaries: test.input}
		setDefaultPerUnitHistogramBoundaries(config)
		s.Equal(test.expectResult, config.PerUnitHistogramBoundaries)
	}
}

func TestMetricsHandlerFromConfig(t *testing.T) {
	t.Parallel()

	logger := log.NewTestLogger()

	for _, c := range []struct {
		name         string
		cfg          *metrics.Config
		expectedType any
	}{
		{
			name:         "nil config",
			cfg:          nil,
			expectedType: metrics.NoopMetricsHandler,
		},
		{
			name: "tally",
			cfg: &metrics.Config{
				Prometheus: &metrics.PrometheusConfig{
					Framework:     metrics.FrameworkTally,
					ListenAddress: "localhost:0",
				},
			},
			expectedType: &tallyMetricsHandler{},
		},
		{
			name: "opentelemetry",
			cfg: &metrics.Config{
				Prometheus: &metrics.PrometheusConfig{
					Framework:     metrics.FrameworkOpentelemetry,
					ListenAddress: "localhost:0",
				},
			},
			expectedType: &otelMetricsHandler{},
		},
	} {
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
