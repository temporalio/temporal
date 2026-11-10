package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestWorkflowBufferedInputFlushLatency(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		timeSkipping   bool
		wantRecordings int
	}{
		{"normal workflow", false, 1},
		{"time-skipped workflow", true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			ms := historyi.NewMockMutableState(ctrl)
			info := &persistencespb.WorkflowExecutionInfo{}
			if tc.timeSkipping {
				info.TimeSkippingInfo = &persistencespb.TimeSkippingInfo{}
			}
			ms.EXPECT().GetExecutionInfo().Return(info)
			if !tc.timeSkipping {
				ms.EXPECT().Now().Return(now)
				ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
			}
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			c := &ContextImpl{MutableState: ms, metricsHandler: metricsHandler}
			c.emitWorkflowBufferedInputFlushLatency([]*historypb.HistoryEvent{
				{EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, EventTime: timestamppb.New(now.Add(-2 * time.Second))},
				{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, EventTime: timestamppb.New(now.Add(-2 * time.Second))},
			})
			recordings := capture.SnapshotMetric(metrics.WorkflowBufferedInputFlushLatency.Name())
			require.Len(t, recordings, tc.wantRecordings)
			if !tc.timeSkipping {
				require.Equal(t, 2*time.Second, recordings[0].Value)
				require.Equal(t, "EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED", recordings[0].Tags["event_type"])
				require.Equal(t, tests.GlobalNamespaceEntry.Name().String(), recordings[0].Tags["namespace"])
			}
		})
	}
}
