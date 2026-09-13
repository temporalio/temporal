package testcore

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/parallelsuite"
)

type MetricCaptureSuite struct {
	parallelsuite.Suite[*MetricCaptureSuite]
}

func TestMetricCaptureSuite(t *testing.T) {
	parallelsuite.Run(t, &MetricCaptureSuite{})
}

func TestNamespaceMetricCapture(t *testing.T) {
	t.Parallel()
	for _, namespace := range []string{"test-ns", "other-ns", ""} {
		t.Run(namespace, func(t *testing.T) {
			t.Parallel()
			handler := metricstest.NewCaptureHandler()
			capture := newNamespaceMetricCapture(handler, namespace)
			t.Cleanup(func() { handler.StopCapture(capture.capture) })
			for range 100 {
				handler.WithTags(metrics.NamespaceTag("unrelated-ns")).Counter("namespaced_metric").Record(1)
				handler.Counter("cluster_metric").Record(1)
			}
			handler.WithTags(metrics.StringTag("namespace", namespace)).Counter("namespaced_metric").Record(2)
			handler.Counter("namespaced_metric").Record(3, metrics.StringTag("namespace", namespace))
			expected := []*metricstest.CapturedRecording{
				{Value: int64(2), Tags: map[string]string{"namespace": namespace}},
				{Value: int64(3), Tags: map[string]string{"namespace": namespace}},
			}
			require.Equal(t, metricstest.CaptureSnapshot{"namespaced_metric": expected}, capture.capture.Snapshot())
			require.Equal(t, expected, capture.Metric("namespaced_metric"))
			require.Equal(t, expected[1:], capture.CollectMetric("namespaced_metric", func(rec *metricstest.CapturedRecording) bool {
				return rec.Value == int64(3)
			}))
			require.Empty(t, capture.Metric("absent_metric"))
			require.PanicsWithValue(t, collectMetricNilKeepPanic, func() { capture.CollectMetric("namespaced_metric", nil) })
			require.PanicsWithValue(t,
				`metric "cluster_metric" is not namespace-scoped; use GlobalMetricCapture instead`,
				func() { capture.Metric("cluster_metric") },
			)
			handler.Counter("namespaced_metric").Record(4)
			require.PanicsWithValue(t,
				`metric "namespaced_metric" is not namespace-scoped; use GlobalMetricCapture instead`,
				func() { capture.Metric("namespaced_metric") },
			)
		})
	}
}

func TestNamespaceMetricCaptureConcurrent(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	first := newNamespaceMetricCapture(handler, "first")
	second := newNamespaceMetricCapture(handler, "second")
	t.Cleanup(func() {
		handler.StopCapture(first.capture)
		handler.StopCapture(second.capture)
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				handler.Counter("counter").Record(1, metrics.NamespaceTag("first"))
				handler.Counter("counter").Record(2, metrics.NamespaceTag("second"))
				handler.Counter("global").Record(3)
				first.Metric("counter")
				second.Metric("counter")
			}
		})
	}
	wg.Wait()
	for _, capture := range []*NamespaceMetricCapture{first, second} {
		require.Len(t, capture.Metric("counter"), 800)
		require.Len(t, capture.capture.Snapshot(), 1)
		require.PanicsWithValue(t,
			`metric "global" is not namespace-scoped; use GlobalMetricCapture instead`,
			func() { capture.Metric("global") },
		)
	}
}

func (s *MetricCaptureSuite) TestGlobalMetricCapture() {
	s.Run("panics when only namespace-scoped metrics were queried", func(s *MetricCaptureSuite) {
		handler := metricstest.NewCaptureHandler()
		capture := handler.StartCapture()
		s.T().Cleanup(func() {
			handler.StopCapture(capture)
		})

		globalCapture := newGlobalMetricCapture(capture)
		handler.WithTags(metrics.NamespaceTag("test-ns")).Counter("namespaced_metric").Record(1)

		recordings := globalCapture.Metric("namespaced_metric")
		s.Len(recordings, 1)

		s.PanicsWithValue(
			"GlobalMetricCapture was used, but all queried metrics were namespace-scoped; use NamespaceMetricCapture instead",
			func() {
				globalCapture.checkForNamespaceCaptureMisuse()
			},
		)
	})

	s.Run("does not panic when a non-namespace metric was queried", func(s *MetricCaptureSuite) {
		handler := metricstest.NewCaptureHandler()
		capture := handler.StartCapture()
		s.T().Cleanup(func() {
			handler.StopCapture(capture)
		})

		globalCapture := newGlobalMetricCapture(capture)
		handler.Counter("cluster_metric").Record(1)

		recordings := globalCapture.Metric("cluster_metric")
		s.Len(recordings, 1)
		s.NotPanics(func() {
			globalCapture.checkForNamespaceCaptureMisuse()
		})
	})
}
