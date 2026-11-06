package activity

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/api/matchingservicemock/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/activity/gen/activitypb/v1"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	test "go.temporal.io/server/common/testing"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestGrantEagerActivityDispatch(t *testing.T) {
	t.Parallel()

	priority := &commonpb.Priority{PriorityKey: 2, FairnessKey: "fairness-key"}
	request := &workflowservice.StartActivityExecutionRequest{
		Namespace: "namespace",
		TaskQueue: &taskqueuepb.TaskQueue{Name: "activity-task-queue"},
		Priority:  priority,
	}

	testCases := []struct {
		name                    string
		response                *matchingservice.GrantEagerDispatchResponse
		err                     error
		granted                 bool
		noClient                bool
		disableTaskQueueMetrics bool
	}{
		{
			name: "granted",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}},
			granted: true,
		},
		{
			name: "denied",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{},
			}},
		},
		{
			name: "matching error",
			err:  errors.New("matching unavailable"),
		},
		{
			name:     "missing response item",
			response: &matchingservice.GrantEagerDispatchResponse{},
		},
		{
			name:     "missing client",
			noClient: true,
		},
		{
			name: "task queue metrics omitted",
			response: &matchingservice.GrantEagerDispatchResponse{Items: []*matchingservice.GrantEagerDispatchResponse_Item{
				{GrantedCount: 1},
			}},
			granted:                 true,
			disableTaskQueueMetrics: true,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			t.Cleanup(func() { metricsHandler.StopCapture(capture) })
			h := &handler{
				metricsHandler: metricsHandler,
				config: &Config{
					BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(!test.disableTaskQueueMetrics),
				},
			}
			if test.noClient {
				require.False(t, h.grantEagerActivityDispatch(t.Context(), "namespace-id", request))
				require.Empty(t, capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name()))
				return
			}

			controller := gomock.NewController(t)
			matchingClient := matchingservicemock.NewMockMatchingServiceClient(controller)
			matchingClient.EXPECT().GrantEagerDispatch(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, actual *matchingservice.GrantEagerDispatchRequest, _ ...grpc.CallOption) (*matchingservice.GrantEagerDispatchResponse, error) {
					require.Equal(t, "namespace-id", actual.GetNamespaceId())
					require.Equal(t, "activity-task-queue", actual.GetTaskQueuePartition().GetTaskQueue())
					require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, actual.GetTaskQueuePartition().GetTaskQueueType())
					require.Len(t, actual.GetItems(), 1)
					require.EqualValues(t, 1, actual.GetItems()[0].GetCount())
					require.True(t, proto.Equal(priority, actual.GetItems()[0].GetPriority()))
					return test.response, test.err
				},
			)

			h.matchingClient = matchingClient
			require.Equal(t, test.granted, h.grantEagerActivityDispatch(t.Context(), "namespace-id", request))

			requestMetrics := capture.SnapshotMetric(metrics.EagerDispatchRequestsSent.Name())
			require.Len(t, requestMetrics, 1)
			require.Equal(t, int64(1), requestMetrics[0].Value)
			expectedTaskQueue := "activity-task-queue"
			if test.disableTaskQueueMetrics {
				expectedTaskQueue = "__omitted__"
			}
			require.Equal(t, map[string]string{
				metrics.NamespaceTag("").Key:                                   "namespace",
				metrics.UnsafeTaskQueueTag("").Key:                             expectedTaskQueue,
				metrics.OperationTag("").Key:                                   "StartActivityExecution",
				metrics.TaskQueueTypeTag(enumspb.TASK_QUEUE_TYPE_ACTIVITY).Key: "Activity",
			}, requestMetrics[0].Tags)
		})
	}
}

// newStartTestHandler returns a handler wired to the CHASM test engine, the context to call it with, and
// a capture of the metrics it records.
func newStartTestHandler(t *testing.T) (*handler, context.Context, *metricstest.Capture) {
	t.Helper()
	config := &Config{
		EnableActivityEagerDispatchCheck: func(string) bool { return false },
		BreakdownMetricsByTaskQueue:      func(string, string, enumspb.TaskQueueType) bool { return true },
	}
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(callback.NewNilLibrary()))
	require.NoError(t, registry.Register(&library{componentOnlyLibrary: *newComponentOnlyLibrary(config)}))

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	t.Cleanup(func() { metricsHandler.StopCapture(capture) })

	maxLinks := func(string) int { return 10 }
	h := &handler{
		config:            config,
		callbackValidator: test.NewCallbacksValidator(t, test.NewCallbacksValidatorConfig()),
		linkValidator:     newLinkValidator(maxLinks, maxLinks, func(string) int { return 4000 }),
		logger:            log.NewTestLogger(),
		metricsHandler:    metricsHandler,
	}
	return h, chasm.NewEngineContext(context.Background(), chasmtest.NewEngine(t, registry)), capture
}

// startWithAttach calls StartActivityExecution with a completion callback and on_conflict_options that
// attach it, which goes through UpdateWithStartExecution.
func startWithAttach(
	ctx context.Context,
	h *handler,
	requestID string,
	conflictPolicy enumspb.ActivityIdConflictPolicy,
) (*workflowservice.StartActivityExecutionResponse, error) {
	resp, err := h.StartActivityExecution(ctx, &activitypb.StartActivityExecutionRequest{
		NamespaceId: "namespace-id",
		FrontendRequest: &workflowservice.StartActivityExecutionRequest{
			Namespace:           "namespace",
			ActivityId:          "activity-id",
			ActivityType:        &commonpb.ActivityType{Name: "activity-type"},
			TaskQueue:           &taskqueuepb.TaskQueue{Name: "activity-task-queue"},
			StartToCloseTimeout: durationpb.New(time.Minute),
			RequestId:           requestID,
			IdReusePolicy:       enumspb.ACTIVITY_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			IdConflictPolicy:    conflictPolicy,
			CompletionCallbacks: []*commonpb.Callback{newNexusCallback()},
			OnConflictOptions: &commonpb.OnConflictOptions{
				AttachRequestId:           true,
				AttachCompletionCallbacks: true,
			},
			PropagatedNexusSerializationContext: &nexuspb.PropagatedSerializationContext{
				Endpoint:  "endpoint",
				Service:   "service",
				Operation: "operation",
			},
		},
	})
	return resp.GetFrontendResponse(), err
}

func callbackCount(ctx context.Context, t *testing.T) int {
	t.Helper()
	ref := chasm.NewComponentRef[*Activity](chasm.ExecutionKey{NamespaceID: "namespace-id", BusinessID: "activity-id"})
	count, err := chasm.ReadComponent(ctx, ref, func(a *Activity, _ chasm.Context, _ any) (int, error) {
		return len(a.Callbacks), nil
	}, nil)
	require.NoError(t, err)
	return count
}

// TestStartActivityExecution_UseExistingMetric verifies that the attach update, which also runs on a new
// activity, records NexusActivityUseExisting only when it attaches to an existing activity.
func TestStartActivityExecution_UseExistingMetric(t *testing.T) {
	h, ctx, capture := newStartTestHandler(t)

	resp, err := startWithAttach(ctx, h, "request-1", enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)
	require.True(t, resp.GetStarted())
	require.Equal(t, 1, callbackCount(ctx, t), "the update must not attach the new activity's callback again")
	require.Empty(t, capture.SnapshotMetric(metrics.NexusActivityUseExisting.Name()))

	resp, err = startWithAttach(ctx, h, "request-2", enumspb.ACTIVITY_ID_CONFLICT_POLICY_USE_EXISTING)
	require.NoError(t, err)
	require.False(t, resp.GetStarted())
	require.Equal(t, 2, callbackCount(ctx, t))
	require.Len(t, capture.SnapshotMetric(metrics.NexusActivityUseExisting.Name()), 1)
}

// TestStartActivityExecution_FailRejectsAttach verifies that under ACTIVITY_ID_CONFLICT_POLICY_FAIL,
// on_conflict_options do not attach to a running activity.
func TestStartActivityExecution_FailRejectsAttach(t *testing.T) {
	h, ctx, capture := newStartTestHandler(t)

	first, err := startWithAttach(ctx, h, "request-1", enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL)
	require.NoError(t, err)

	_, err = startWithAttach(ctx, h, "request-2", enumspb.ACTIVITY_ID_CONFLICT_POLICY_FAIL)
	var alreadyStarted *serviceerror.ActivityExecutionAlreadyStarted
	require.ErrorAs(t, err, &alreadyStarted)
	require.Equal(t, first.GetRunId(), alreadyStarted.RunId)
	require.Equal(t, "request-1", alreadyStarted.StartRequestId)
	require.Equal(t, 1, callbackCount(ctx, t))
	require.Empty(t, capture.SnapshotMetric(metrics.NexusActivityUseExisting.Name()))
}
