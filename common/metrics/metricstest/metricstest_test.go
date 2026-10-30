package metricstest

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

func TestHandler(t *testing.T) {
	t.Parallel()

	systemTags := []metrics.Tag{
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	}

	t.Run("counter", func(t *testing.T) {
		t.Parallel()
		handler, err := NewHandler(log.NewTestLogger(), metrics.ClientConfig{})
		require.NoError(t, err)

		tags := []metrics.Tag{metrics.StringTag("k1", "v1")}
		handler.WithTags(tags...).Counter("my_counter").Record(1)
		handler.WithTags(tags...).Counter("my_counter").Record(1)

		snap, err := handler.Snapshot()
		require.NoError(t, err)

		val, err := snap.Counter("my_counter_total", append(systemTags, tags...)...)
		require.NoError(t, err)
		assert.InDelta(t, 2, val, 0.0001)
	})

	t.Run("gauge", func(t *testing.T) {
		t.Parallel()
		handler, err := NewHandler(log.NewTestLogger(), metrics.ClientConfig{})
		require.NoError(t, err)

		tags := []metrics.Tag{metrics.StringTag("k1", "v1")}
		handler.WithTags(tags...).Gauge("my_gauge").Record(-2)
		handler.WithTags(tags...).Gauge("my_gauge").Record(10)

		snap, err := handler.Snapshot()
		require.NoError(t, err)

		val, err := snap.Gauge("my_gauge", append(systemTags, tags...)...)
		require.NoError(t, err)
		assert.InDelta(t, 10, val, 0.0001)
	})

	t.Run("histogram", func(t *testing.T) {
		t.Parallel()
		handler, err := NewHandler(log.NewTestLogger(), metrics.ClientConfig{
			PerUnitHistogramBoundaries: map[string][]float64{
				metrics.Dimensionless: {1, 2, 5},
			},
		})
		require.NoError(t, err)

		tags := []metrics.Tag{metrics.StringTag("k1", "v1")}
		handler.WithTags(tags...).Histogram("my_histogram", metrics.Dimensionless).Record(1)
		handler.WithTags(tags...).Histogram("my_histogram", metrics.Dimensionless).Record(3)

		snap, err := handler.Snapshot()
		require.NoError(t, err)

		buckets, err := snap.Histogram("my_histogram_ratio", append(systemTags, tags...)...)
		require.NoError(t, err)
		assert.Equal(t, []HistogramBucket{
			{value: 1, upperBound: 1},
			{value: 1, upperBound: 2},
			{value: 2, upperBound: 5},
			{value: 2, upperBound: math.Inf(1)},
		}, buckets)
	})
}

func TestCaptureHandler(t *testing.T) {
	t.Run("zero allocations when no capture active", func(t *testing.T) {
		handler := NewCaptureHandler()
		counter := handler.Counter("test")

		allocs := testing.AllocsPerRun(1, func() {
			counter.Record(1)
		})
		assert.Zero(t, allocs)
	})

	t.Run("discards metrics when no capture active", func(t *testing.T) {
		t.Parallel()
		handler := NewCaptureHandler()

		handler.Counter("test_counter").Record(1)
		handler.Gauge("test_gauge").Record(42.0)

		capture := handler.StartCapture()
		defer handler.StopCapture(capture)
		assert.Empty(t, capture.Snapshot())
	})

	t.Run("records metrics when capture active", func(t *testing.T) {
		t.Parallel()
		handler := NewCaptureHandler()

		capture := handler.StartCapture()
		defer handler.StopCapture(capture)

		handler.Counter("test_counter").Record(1)
		handler.Counter("test_counter").Record(2)
		handler.Gauge("test_gauge").Record(42.0)

		snapshot := capture.Snapshot()
		assert.Len(t, snapshot["test_counter"], 2)
		assert.Len(t, snapshot["test_gauge"], 1)
	})
}
