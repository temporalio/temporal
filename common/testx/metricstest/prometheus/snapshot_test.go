package prometheus_test

import (
	"errors"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	promtest "go.temporal.io/testx/metricstest/prometheus"
)

func TestGather(t *testing.T) {
	t.Parallel()

	labels := prometheus.Labels{"tenant": "one"}
	counter := prometheus.NewCounter(prometheus.CounterOpts{Name: "requests_total", Help: "Requests", ConstLabels: labels})
	gauge := prometheus.NewGauge(prometheus.GaugeOpts{Name: "inflight", Help: "Inflight", ConstLabels: labels})
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "latency", Help: "Latency", ConstLabels: labels, Buckets: []float64{1, 2, 5},
	})
	registry := prometheus.NewRegistry()
	require.NoError(t, registry.Register(counter))
	require.NoError(t, registry.Register(gauge))
	require.NoError(t, registry.Register(histogram))

	counter.Add(2)
	gauge.Set(-2)
	gauge.Set(10)
	histogram.Observe(1)
	histogram.Observe(3)

	snapshot, err := promtest.Gather(registry)
	require.NoError(t, err)

	count, err := snapshot.Counter("requests_total", labels)
	require.NoError(t, err)
	require.InDelta(t, 2, count, 0.0001)

	value, err := snapshot.Gauge("inflight", labels)
	require.NoError(t, err)
	require.InDelta(t, 10, value, 0.0001)

	buckets, err := snapshot.Histogram("latency", labels)
	require.NoError(t, err)
	require.Equal(t, []promtest.HistogramBucket{
		{Value: 1, UpperBound: 1},
		{Value: 1, UpperBound: 2},
		{Value: 2, UpperBound: 5},
		{Value: 2, UpperBound: math.Inf(1)},
	}, buckets)
	require.Contains(t, snapshot.String(), "requests_total")
	require.Contains(t, snapshot.String(), "latency")

	for _, tc := range []struct {
		name  string
		query func() error
		want  error
	}{
		{
			name: "missing metric",
			query: func() error {
				_, err := snapshot.Counter("missing", nil)
				return err
			},
			want: promtest.ErrMetricNotFound,
		},
		{
			name: "wrong metric type",
			query: func() error {
				_, err := snapshot.Gauge("requests_total", labels)
				return err
			},
			want: promtest.ErrMetricTypeMismatch,
		},
		{
			name: "wrong scalar labels",
			query: func() error {
				_, err := snapshot.Counter("requests_total", map[string]string{"tenant": "two"})
				return err
			},
			want: promtest.ErrMetricLabelMismatch,
		},
		{
			name: "wrong histogram labels",
			query: func() error {
				_, err := snapshot.Histogram("latency", nil)
				return err
			},
			want: promtest.ErrMetricLabelMismatch,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tc.query(), tc.want)
		})
	}
}

func TestGatherError(t *testing.T) {
	t.Parallel()

	gatherErr := errors.New("collection failed")
	gatherer := prometheus.GathererFunc(func() ([]*dto.MetricFamily, error) {
		return nil, gatherErr
	})

	snapshot, err := promtest.Gather(gatherer)
	require.ErrorIs(t, err, gatherErr)
	require.Equal(t, promtest.Snapshot{}, snapshot)
}
