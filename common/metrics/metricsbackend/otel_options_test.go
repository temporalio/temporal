package metricsbackend

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/server/common/metrics"
)

var (
	metricWithoutOptions = metrics.NewCounterDef("metricsbackend_test_metric_without_options")
	metricWithOptions    = metrics.NewCounterDef(
		"metricsbackend_test_metric_with_options",
		metrics.WithDescription("bar description"),
		metrics.WithUnit(metrics.Bytes),
	)
)

type testCase struct {
	name         string
	metricName   string
	expectedOpts []metric.InstrumentOption
}

func TestAddOptions(t *testing.T) {
	t.Parallel()

	catalog, err := metrics.BuildCatalog()
	require.NoError(t, err)
	inputOpts := []metric.InstrumentOption{
		metric.WithDescription("foo description"),
		metric.WithUnit(metrics.Milliseconds),
	}
	for _, c := range []testCase{
		{
			name:         "missing metric",
			metricName:   "metricsbackend_test_metric_not_defined",
			expectedOpts: inputOpts,
		},
		{
			name:         "empty metric definition",
			metricName:   metricWithoutOptions.Name(),
			expectedOpts: inputOpts,
		},
		{
			name:       "opts overwritten",
			metricName: metricWithOptions.Name(),
			expectedOpts: []metric.InstrumentOption{
				metric.WithDescription("foo description"),
				metric.WithUnit(metrics.Milliseconds),
				metric.WithDescription("bar description"),
				metric.WithUnit(metrics.Bytes),
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			handler := &otelMetricsHandler{catalog: catalog}
			var (
				counter     counterOptions
				gauge       gaugeOptions
				int64hist   int64HistogramOptions
				float64hist float64HistogramOptions
			)
			for _, opt := range inputOpts {
				counter = append(counter, opt.(metric.Int64CounterOption))
				gauge = append(gauge, opt.(metric.Float64ObservableGaugeOption))
				int64hist = append(int64hist, opt.(metric.Int64HistogramOption))
				float64hist = append(float64hist, opt.(metric.Float64HistogramOption))
			}
			counter = addOptions(handler, counter, c.metricName)
			gauge = addOptions(handler, gauge, c.metricName)
			int64hist = addOptions(handler, int64hist, c.metricName)
			float64hist = addOptions(handler, float64hist, c.metricName)
			require.Len(t, counter, len(c.expectedOpts))
			require.Len(t, gauge, len(c.expectedOpts))
			require.Len(t, int64hist, len(c.expectedOpts))
			require.Len(t, float64hist, len(c.expectedOpts))
			for i, opt := range c.expectedOpts {
				assert.Equal(t, opt, counter[i])
				assert.Equal(t, opt, gauge[i])
				assert.Equal(t, opt, int64hist[i])
				assert.Equal(t, opt, float64hist[i])
			}
		})
	}
}
