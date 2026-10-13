package metrics

import (
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
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
	statsd := &StatsdConfig{
		HostPort: "127.0.0.1:8125",
		Prefix:   "testStatsd",
	}

	config := new(Config)
	config.Statsd = statsd
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

func TestPrometheusReporterErrors(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		address string
		tags    []map[string]string
		wantLog testlogger.CapturedLogPattern
	}{
		{
			name:    "label collision",
			address: "127.0.0.1:0",
			tags: []map[string]string{
				{"outcome": "fired"},
				{"outcome": "skipped"},
				{"outcome": "invalidated", "reason": "closed"},
			},
			wantLog: testlogger.CapturedLogPattern{
				Level:   testlogger.Error,
				Message: "failed assertion: error in prometheus reporter",
				Tags:    map[string]any{"failed-assertion": true, "error": testlogger.AnyTagValue},
			},
		},
		{
			name:    "listener error",
			address: "127.0.0.1:65536",
			wantLog: testlogger.CapturedLogPattern{
				Level:   testlogger.Warn,
				Message: "error in prometheus reporter",
				Tags:    map[string]any{"error": testlogger.AnyTagValue},
			},
		},
		{
			name:    "invalid label",
			address: "127.0.0.1:0",
			tags:    []map[string]string{{"__reserved": "value"}},
			wantLog: testlogger.CapturedLogPattern{
				Level:   testlogger.Warn,
				Message: "error in prometheus reporter",
				Tags:    map[string]any{"error": testlogger.AnyTagValue},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			capture := logger.StartCapture()
			t.Cleanup(func() { logger.StopCapture(capture) })
			scope := NewScope(logger, &Config{
				Prometheus: &PrometheusConfig{ListenAddress: tc.address},
			})
			t.Cleanup(func() {
				require.NoError(t, scope.(io.Closer).Close())
			})

			for _, tags := range tc.tags {
				scope.Tagged(tags).Counter("test_counter").Inc(1)
			}
			await.RequireTrue(t, func() bool { return len(capture.Snapshot()) > 0 }, time.Second, 10*time.Millisecond)
			require.Len(t, capture.Snapshot(), 1)
			capture.RequireContains(t, tc.wantLog)
		})
	}
}

func TestMetricsHandlerFromConfig(t *testing.T) {
	t.Parallel()

	logger := log.NewTestLogger()

	for _, c := range []struct {
		name         string
		cfg          *Config
		expectedType any
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
