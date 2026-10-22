package visibility

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.uber.org/mock/gomock"
)

func TestVisibilityPersistenceMetricsArchetype(t *testing.T) {
	for _, archetype := range []string{"concurrency_limiter", "activity", ""} {
		t.Run(archetype, func(t *testing.T) {
			delegate := manager.NewMockVisibilityManager(gomock.NewController(t))
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			client := NewVisibilityManagerMetrics(delegate, handler, log.NewNoopLogger(),
				func() time.Duration { return time.Second }, metrics.VisibilityPluginNameTag("sql"), metrics.VisibilityIndexNameTag("visibility"))
			ctx := context.Background()
			want := "_unknown_"
			if archetype != "" {
				ctx = metrics.WithArchetypeContext(ctx, archetype)
				want = archetype
			}
			errExpected := serviceerror.NewUnavailable("unavailable")
			delegate.EXPECT().RecordWorkflowExecutionStarted(ctx, gomock.Any()).Return(errExpected)
			require.ErrorIs(t, client.RecordWorkflowExecutionStarted(ctx, &manager.RecordWorkflowExecutionStartedRequest{}), errExpected)
			snapshot := capture.Snapshot()
			for _, name := range []string{
				metrics.VisibilityPersistenceRequests.Name(), metrics.VisibilityPersistenceLatency.Name(),
				metrics.VisibilityPersistenceErrorWithType.Name(), metrics.VisibilityPersistenceFailures.Name(),
			} {
				require.Len(t, snapshot[name], 1)
				require.Equal(t, want, snapshot[name][0].Tags["archetype"])
			}
		})
	}
}
