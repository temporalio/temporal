package metricstest_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/testx/metricstest"
)

type recording struct {
	Value any
	Tags  map[string]string
}

func TestRecorderFilter(t *testing.T) {
	t.Parallel()
	recorder := metricstest.NewRecorder[recording]()
	all := recorder.StartCapture()
	filtered := recorder.StartCaptureWithFilter(func(name string, rec *recording) bool {
		return name == "counter" && rec.Tags["namespace"] == "wanted"
	})
	t.Cleanup(func() {
		recorder.StopCapture(all)
		recorder.StopCapture(filtered)
	})

	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(1), Tags: map[string]string{"namespace": "other"}}
	})
	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(2), Tags: map[string]string{"namespace": "wanted"}}
	})
	recorder.Record("other_metric", func() *recording {
		return &recording{Value: int64(3), Tags: map[string]string{"namespace": "wanted"}}
	})
	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(4)}
	})

	require.Equal(t, metricstest.CaptureSnapshot[recording]{"counter": {
		{Value: int64(2), Tags: map[string]string{"namespace": "wanted"}},
	}}, filtered.Snapshot())
	require.Len(t, all.SnapshotMetric("counter"), 3)
	require.Len(t, all.SnapshotMetric("other_metric"), 1)
}

func TestCaptureSnapshotMetric(t *testing.T) {
	t.Parallel()
	recorder := metricstest.NewRecorder[recording]()
	capture := recorder.StartCapture()
	defer recorder.StopCapture(capture)
	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(1)}
	})
	recorder.Record("other", func() *recording {
		return &recording{Value: int64(2)}
	})

	snapshot := capture.SnapshotMetric("counter")
	require.Equal(t, capture.Snapshot()["counter"], snapshot)
	require.Nil(t, capture.SnapshotMetric("missing"))
	snapshot[0] = nil
	require.NotNil(t, capture.SnapshotMetric("counter")[0])
	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(3)}
	})
	require.Len(t, snapshot, 1)
	require.Len(t, capture.SnapshotMetric("counter"), 2)
}

func TestCaptureFilterConcurrentRecording(t *testing.T) {
	t.Parallel()
	recorder := metricstest.NewRecorder[recording]()
	capture := recorder.StartCaptureWithFilter(func(_ string, rec *recording) bool {
		return rec.Tags["namespace"] == "wanted"
	})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				recorder.Record("counter", func() *recording {
					return &recording{Value: int64(1), Tags: map[string]string{"namespace": "wanted"}}
				})
				recorder.Record("counter", func() *recording {
					return &recording{Value: int64(2), Tags: map[string]string{"namespace": "other"}}
				})
				capture.SnapshotMetric("counter")
			}
		})
	}
	wg.Wait()
	recorder.StopCapture(capture)
	recorder.Record("counter", func() *recording {
		return &recording{Value: int64(3), Tags: map[string]string{"namespace": "wanted"}}
	})
	require.Len(t, capture.SnapshotMetric("counter"), 800)
}

func TestRecorderDoesNotBuildWithoutCaptures(t *testing.T) {
	t.Parallel()

	recorder := metricstest.NewRecorder[recording]()
	builds := 0
	build := func() *recording {
		builds++
		return &recording{Value: int64(1)}
	}

	recorder.Record("counter", build)
	require.Zero(t, builds)

	capture := recorder.StartCapture()
	recorder.Record("counter", build)
	require.Equal(t, 1, builds)
	require.Equal(t, []*recording{{Value: int64(1)}}, capture.SnapshotMetric("counter"))

	recorder.StopCapture(capture)
	recorder.Record("counter", build)
	require.Equal(t, 1, builds)
}
