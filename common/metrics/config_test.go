package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/log"
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

func (s *MetricsSuite) TestSetDefaultPerUnitHistogramBoundaries() {
	type histogramTest struct {
		input        map[string][]float64
		expectResult map[string][]float64
	}

	testCases := []histogramTest{
		{
			input: nil,
			expectResult: map[string][]float64{
				Dimensionless: defaultPerUnitHistogramBoundaries[Dimensionless],
				Milliseconds:  defaultPerUnitHistogramBoundaries[Milliseconds],
				Seconds:       {0.001, 0.002, 0.005, 0.010, 0.020, 0.050, 0.100, 0.200, 0.500, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1_000},
				Bytes:         defaultPerUnitHistogramBoundaries[Bytes],
			},
		},
		{
			input: map[string][]float64{
				UnitNameDimensionless: {1},
				UnitNameMilliseconds:  {10, 1000, 2000},
				"notDefine":           {1},
			},
			expectResult: map[string][]float64{
				Dimensionless: {1},
				Milliseconds:  {10, 1000, 2000},
				Seconds:       {0.01, 1, 2},
				Bytes:         defaultPerUnitHistogramBoundaries[Bytes],
			},
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
			name: "prometheus config",
			cfg: &Config{
				Prometheus: &PrometheusConfig{
					ListenAddress: "localhost:0",
				},
			},
			expectedType: &otelMetricsHandler{},
		},
		{
			name:         "empty config",
			cfg:          &Config{},
			expectedType: &noopMetricsHandler{},
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
