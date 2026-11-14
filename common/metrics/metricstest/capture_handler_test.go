package metricstest

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/metrics"
)

func TestCaptureHandlerFilter(t *testing.T) {
	t.Parallel()
	handler := NewCaptureHandler()
	all := handler.StartCapture()
	filtered := handler.StartCaptureWithFilter(func(name string, rec *CapturedRecording) bool {
		return name == "counter" && rec.Tags["namespace"] == "wanted"
	})
	t.Cleanup(func() {
		handler.StopCapture(all)
		handler.StopCapture(filtered)
	})

	tagged := handler.WithTags(metrics.NamespaceTag("other"))
	tagged.Counter("counter").Record(1)
	tagged.Counter("counter").Record(2, metrics.NamespaceTag("wanted"))
	tagged.Counter("other_metric").Record(3, metrics.NamespaceTag("wanted"))
	handler.Counter("counter").Record(4)

	require.Equal(t, CaptureSnapshot{"counter": {
		{Value: int64(2), Tags: map[string]string{"namespace": "wanted"}},
	}}, filtered.Snapshot())
	require.Len(t, all.SnapshotMetric("counter"), 3)
	require.Len(t, all.SnapshotMetric("other_metric"), 1)
}
