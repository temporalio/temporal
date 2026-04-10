package testcore

import (
	"testing"

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

func (s *MetricCaptureSuite) TestNamespaceMetricCapture() {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	s.T().Cleanup(func() {
		handler.StopCapture(capture)
	})

	s.Run("returns only recordings for the test namespace", func(s *MetricCaptureSuite) {
		// Record the same metric for two namespaces.
		const metricName = "namespaced_metric"
		metricsHandler := handler.WithTags(metrics.NamespaceTag("test-ns"))
		metricsHandler.Counter(metricName).Record(1)
		handler.WithTags(metrics.NamespaceTag("other-ns")).Counter(metricName).Record(1)

		namespaceCapture := newNamespaceMetricCapture(capture, "test-ns")

		recordings := namespaceCapture.Metric(metricName)
		s.Len(recordings, 1)
		s.Equal("test-ns", recordings[0].Tags["namespace"])
	})

	s.Run("panics when the metric is not namespace-scoped", func(s *MetricCaptureSuite) {
		// Record the metric without a namespace tag.
		handler.Counter("cluster_metric").Record(1)

		namespaceCapture := newNamespaceMetricCapture(capture, "test-ns")

		s.PanicsWithValue(
			`metric "cluster_metric" is not namespace-scoped; use GlobalMetricCapture instead`,
			func() {
				namespaceCapture.Metric("cluster_metric")
			},
		)
	})
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
