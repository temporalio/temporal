package metrics

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
)

type testCase struct {
	name         string
	catalog      map[string]metricDefinition
	expectedOpts []metric.InstrumentOption
}

func TestAddOptions(t *testing.T) {
	t.Parallel()

	metricName := "foo"
	inputOpts := []metric.InstrumentOption{
		metric.WithDescription("foo description"),
		metric.WithUnit(Milliseconds),
	}
	for _, c := range []testCase{
		{
			name:         "missing metric",
			catalog:      map[string]metricDefinition{},
			expectedOpts: inputOpts,
		},
		{
			name: "empty metric definition",
			catalog: map[string]metricDefinition{
				metricName: {},
			},
			expectedOpts: inputOpts,
		},
		{
			name: "opts overwritten",
			catalog: map[string]metricDefinition{
				metricName: {
					description: "bar description",
					unit:        Bytes,
				},
			},
			expectedOpts: []metric.InstrumentOption{
				metric.WithDescription("foo description"),
				metric.WithUnit(Milliseconds),
				metric.WithDescription("bar description"),
				metric.WithUnit(Bytes),
			},
		},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()

			handler := &otelMetricsHandler{catalog: c.catalog}
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
			counter = addOptions(handler, counter, metricName)
			gauge = addOptions(handler, gauge, metricName)
			int64hist = addOptions(handler, int64hist, metricName)
			float64hist = addOptions(handler, float64hist, metricName)
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
