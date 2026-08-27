package activity

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestCompletionMetricsWorkerDeploymentLabelKeyParity(t *testing.T) {
	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(capture)

	ctx := &chasm.MockMutableContext{
		MockContext: chasm.MockContext{
			HandleMetricsHandler: func() metrics.Handler { return metricsHandler },
			HandleNamespaceEntry: testNamespaceEntry,
			GoCtx: context.WithValue(context.Background(), ctxKeyActivityContext, &activityContext{
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				},
			}),
		},
	}
	activity := &Activity{
		ActivityState: &activitypb.ActivityState{
			ActivityType: &commonpb.ActivityType{Name: "test-activity-type"},
			ScheduleTime: timestamppb.New(defaultTime),
			TaskQueue:    &taskqueuepb.TaskQueue{Name: "test-task-queue"},
		},
		LastAttempt: chasm.NewDataField(ctx, &activitypb.ActivityAttemptState{
			StartedTime: timestamppb.New(defaultTime),
		}),
	}

	baseHandler := activity.baseMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	completionHandler := activity.completionMetricsHandler(ctx, metrics.HistoryRespondActivityTaskCompletedScope)
	activity.emitOnCompletedMetrics(ctx, baseHandler, completionHandler, nil, true)
	activity.emitOnFailedMetrics(ctx, baseHandler, completionHandler, &failurepb.Failure{})
	activity.emitOnCanceledMetrics(ctx, completionHandler, activitypb.ACTIVITY_EXECUTION_STATUS_STARTED)
	activity.emitOnTimedOutMetrics(
		completionHandler,
		enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
	)

	expectedTags := []metrics.Tag{
		metrics.WorkerDeploymentNameTag("", false),
		metrics.WorkerDeploymentBuildIDTag("", false),
	}
	for _, metricName := range []string{
		metrics.ActivitySuccess.Name(),
		metrics.ActivityFail.Name(),
		metrics.ActivityTaskFail.Name(),
		metrics.ActivityCancel.Name(),
		metrics.ActivityTaskTimeout.Name(),
		metrics.ActivityTimeout.Name(),
		metrics.ActivityStartToCloseLatency.Name(),
		metrics.ActivityScheduleToCloseLatency.Name(),
	} {
		recordings := capture.Snapshot()[metricName]
		require.NotEmpty(t, recordings, "expected %s to be emitted", metricName)
		for _, recording := range recordings {
			for _, expectedTag := range expectedTags {
				require.Contains(t, recording.Tags, expectedTag.Key, "%s is missing label %s", metricName, expectedTag.Key)
				require.Equal(t, expectedTag.Value, recording.Tags[expectedTag.Key])
			}
		}
	}

	activity.emitOnTerminatedMetrics(activity.enrichedMetricsHandler(ctx, metrics.ActivityTerminatedScope))
	terminated := capture.Snapshot()[metrics.ActivityTerminate.Name()]
	require.Len(t, terminated, 1)
	for _, expectedTag := range expectedTags {
		require.NotContains(t, terminated[0].Tags, expectedTag.Key)
	}
}
