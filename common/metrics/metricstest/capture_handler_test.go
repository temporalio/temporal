package metricstest

import (
	"sync"
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

func TestCaptureSnapshotMetric(t *testing.T) {
	t.Parallel()
	handler := NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	handler.Counter("counter").Record(1)
	handler.Counter("other").Record(2)

	snapshot := capture.SnapshotMetric("counter")
	require.Equal(t, capture.Snapshot()["counter"], snapshot)
	require.Nil(t, capture.SnapshotMetric("missing"))
	snapshot[0] = nil
	require.NotNil(t, capture.SnapshotMetric("counter")[0])
	handler.Counter("counter").Record(3)
	require.Len(t, snapshot, 1)
	require.Len(t, capture.SnapshotMetric("counter"), 2)
}

func TestCaptureFilterConcurrentRecording(t *testing.T) {
	t.Parallel()
	handler := NewCaptureHandler()
	capture := handler.StartCaptureWithFilter(func(_ string, rec *CapturedRecording) bool {
		return rec.Tags["namespace"] == "wanted"
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				handler.Counter("counter").Record(1, metrics.NamespaceTag("wanted"))
				handler.Counter("counter").Record(2, metrics.NamespaceTag("other"))
				capture.SnapshotMetric("counter")
			}
		})
	}
	wg.Wait()
	handler.StopCapture(capture)
	handler.Counter("counter").Record(3, metrics.NamespaceTag("wanted"))
	require.Len(t, capture.SnapshotMetric("counter"), 800)
}

func BenchmarkCaptureSnapshotMetric(b *testing.B) {
	handler := NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	for range 10000 {
		handler.Counter("unrelated").Record(1)
	}
	handler.Counter("queried").Record(1)
	b.Run("all", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			capture.Snapshot()
		}
	})
	b.Run("one", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			capture.SnapshotMetric("queried")
		}
	})
}
