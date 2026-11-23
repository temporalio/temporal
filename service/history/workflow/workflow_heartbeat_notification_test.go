package workflow

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	"go.temporal.io/api/serviceerror"
	workerpb "go.temporal.io/api/worker/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNewWorkflowHeartbeatNotification(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := historyi.NewMockMutableState(ctrl)
	clock := &clockspb.VectorClock{ClusterId: 1, ShardId: 2, Clock: 3}
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowTaskScheduledEventId:   5,
		WorkflowTaskStartedEventId:     6,
		WorkflowTaskStartedTime:        timestamppb.New(time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)),
		WorkflowTaskAttempt:            2,
		WorkflowTaskVersion:            9,
		WorkflowTaskStartedClock:       clock,
		WorkflowTaskWorkerControlQueue: "control-queue",
	})
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)

	notification, err := NewWorkflowHeartbeatNotification(ms, enumspb.EventType_name[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)])
	require.NoError(t, err)
	require.Equal(t, "control-queue", notification.controlQueue)
	token, err := tasktoken.NewSerializer().Deserialize(notification.taskToken)
	require.NoError(t, err)
	require.Equal(t, "namespace-id", token.GetNamespaceId())
	require.Equal(t, int64(5), token.GetScheduledEventId())
	require.Equal(t, int64(6), token.GetStartedEventId())
	require.True(t, proto.Equal(clock, token.GetClock()))
}

func TestNewWorkflowHeartbeatNotification_StartedClockUnknown(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := historyi.NewMockMutableState(ctrl)
	ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowTaskScheduledEventId:   5,
		WorkflowTaskStartedEventId:     6,
		WorkflowTaskWorkerControlQueue: "control-queue",
	})
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"))

	notification, err := NewWorkflowHeartbeatNotification(ms, enumspb.EventType_name[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)])
	require.NoError(t, err)
	require.Nil(t, notification)
}

func TestSendWorkflowHeartbeatNotification(t *testing.T) {
	for _, tc := range []struct {
		name            string
		resp            *matchingservice.DispatchNexusTaskResponse
		err             error
		expectedResult  string
		expectedWarning string
	}{
		{
			name: "worker answered",
			resp: &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
				Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{}},
				}}},
			}},
			expectedResult: "sync_success",
		},
		{
			name: "matching request timeout",
			resp: &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			}},
			expectedResult: "handler_timeout",
		},
		{
			name: "worker failed",
			resp: &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
				Failure: &failurepb.Failure{
					Message: "unsupported command",
					FailureInfo: &failurepb.Failure_NexusHandlerFailureInfo{
						NexusHandlerFailureInfo: &failurepb.NexusHandlerFailureInfo{Type: "NOT_IMPLEMENTED"},
					},
				},
			}},
			expectedResult:  "handler_error:NOT_IMPLEMENTED",
			expectedWarning: "WHN: Worker failed workflow heartbeat notification",
		},
		{
			name:            "dispatch timeout",
			err:             context.DeadlineExceeded,
			expectedResult:  "dispatch_timeout",
			expectedWarning: "WHN: Unable to dispatch workflow heartbeat notification",
		},
		{
			name:            "dispatch error",
			err:             serviceerror.NewUnavailable("unavailable"),
			expectedResult:  "dispatch_error",
			expectedWarning: "WHN: Unable to dispatch workflow heartbeat notification",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			shard := historyi.NewMockShardContext(ctrl)
			shard.EXPECT().GetConfig().Return(&configs.Config{
				WorkflowHeartbeatNotificationRolloutPercent:  func(string) int { return 100 },
				WorkflowHeartbeatNotificationDispatchTimeout: func(string) time.Duration { return 500 * time.Millisecond },
				NexusMinDispatchTaskTimeout:                  func(string) time.Duration { return time.Second },
			}).AnyTimes()
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			shard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
			warnings := make(chan string, 1)
			logger := log.NewMockLogger(ctrl)
			logger.EXPECT().Warn(gomock.Any(), gomock.Any()).Do(func(msg string, _ ...tag.Tag) { warnings <- msg }).AnyTimes()
			shard.EXPECT().GetLogger().Return(logger).AnyTimes()
			delivered := make(chan *matchingservice.DispatchNexusTaskRequest, 1)
			shard.EXPECT().GetMatchingClient().Return(&whnMatchingClient{delivered: delivered, resp: tc.resp, err: tc.err}, nil)
			signaled := enumspb.EventType_name[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)]

			SendWorkflowHeartbeatNotification(context.Background(), shard, &WorkflowHeartbeatNotification{
				workflowKey:        definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"),
				namespaceName:      "namespace-name",
				controlQueue:       "control-queue",
				taskToken:          []byte("task-token"),
				inputEventTypeName: signaled,
			})

			select {
			case request := <-delivered:
				require.Equal(t, "namespace-id", request.GetNamespaceId())
				require.Equal(t, "control-queue", request.GetTaskQueue().GetName())
				var commands workerservicepb.ExecuteCommandsRequest
				require.NoError(t, proto.Unmarshal(request.GetRequest().GetStartOperation().GetPayload().GetData(), &commands))
				require.Equal(t, &workerpb.WorkflowHeartbeatNotificationCommand{
					TaskToken: []byte("task-token"),
					Execution: &commonpb.WorkflowExecution{WorkflowId: "workflow-id", RunId: "run-id"},
				}, commands.GetCommands()[0].GetWorkflowHeartbeatNotification())
			case <-time.After(time.Second):
				t.Fatal("workflow heartbeat notification was not dispatched")
			}
			await.RequireTrue(t, func() bool {
				return len(capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationResults.Name())) == 1
			}, time.Second, 10*time.Millisecond)
			results := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationResults.Name())
			require.Equal(t, map[string]string{
				"namespace":  "namespace-name",
				"event_type": signaled,
				"result":     tc.expectedResult,
			}, results[0].Tags)
			latencies := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationDeliveryLatency.Name())
			require.Len(t, latencies, 1)
			require.Equal(t, map[string]string{"namespace": "namespace-name", "event_type": signaled}, latencies[0].Tags)
			if tc.expectedWarning != "" {
				require.Equal(t, tc.expectedWarning, <-warnings)
			}
		})
	}
}

func TestSendWorkflowHeartbeatNotificationDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		WorkflowHeartbeatNotificationRolloutPercent: func(string) int { return 0 },
	})
	SendWorkflowHeartbeatNotification(context.Background(), shard, &WorkflowHeartbeatNotification{
		namespaceName: "namespace-name",
	})
}

type whnMatchingClient struct {
	matchingservice.MatchingServiceClient
	delivered chan *matchingservice.DispatchNexusTaskRequest
	resp      *matchingservice.DispatchNexusTaskResponse
	err       error
}

func (c *whnMatchingClient) DispatchNexusTask(
	_ context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	_ ...grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	c.delivered <- request
	return c.resp, c.err
}

type bufferedWorkflowHeartbeatNotificationUpdateRegistry struct {
	update.Registry
	hasOutgoing bool
}

func (r bufferedWorkflowHeartbeatNotificationUpdateRegistry) FailoverVersion() int64 { return 1 }
func (r bufferedWorkflowHeartbeatNotificationUpdateRegistry) HasOutgoingMessages(bool) bool {
	return r.hasOutgoing
}

func TestIsWorkflowHeartbeatNotificationInputType(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		eventType enumspb.EventType
		eligible  bool
	}{
		{"signal", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, true},
		{"cancellation", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, true},
		{"timer", enumspb.EVENT_TYPE_TIMER_FIRED, true},
		{"activity completed", enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED, true},
		{"activity failed", enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, true},
		{"activity timed out", enumspb.EVENT_TYPE_ACTIVITY_TASK_TIMED_OUT, true},
		{"activity canceled", enumspb.EVENT_TYPE_ACTIVITY_TASK_CANCELED, true},
		{"child workflow result", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, true},
		{"nexus result", enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, true},
		{"child workflow started", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED, true},
		{"child workflow start failed", enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_FAILED, true},
		{"external signal result", enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_SIGNALED, true},
		{"external signal failed", enumspb.EVENT_TYPE_SIGNAL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, true},
		{"external cancel result", enumspb.EVENT_TYPE_EXTERNAL_WORKFLOW_EXECUTION_CANCEL_REQUESTED, true},
		{"external cancel failed", enumspb.EVENT_TYPE_REQUEST_CANCEL_EXTERNAL_WORKFLOW_EXECUTION_FAILED, true},
		{"nexus started", enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED, true},
		{"nexus cancel request completed", enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED, true},
		{"nexus cancel request failed", enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_FAILED, true},
		{"activity started", enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, false},
		{"unrelated event", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED, false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.eligible, IsWorkflowHeartbeatNotificationInputType(tc.eventType))
		})
	}
}

func TestIsWorkflowHeartbeatNotificationEnabled(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name    string
		want    bool
		percent int
	}{
		{"zero percent disables all", false, 0},
		{"hundred percent enables all", true, 100},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shard := historyi.NewMockShardContext(gomock.NewController(t))
			shard.EXPECT().GetConfig().Return(&configs.Config{
				WorkflowHeartbeatNotificationRolloutPercent: func(string) int { return tc.percent },
			})
			require.Equal(t, tc.want, isWorkflowHeartbeatNotificationEnabled(shard, "namespace", "workflow"))
		})
	}
}

func TestWorkflowHeartbeatNotificationRolloutIsSticky(t *testing.T) {
	t.Parallel()
	shard := historyi.NewMockShardContext(gomock.NewController(t))
	shard.EXPECT().GetConfig().Return(&configs.Config{
		WorkflowHeartbeatNotificationRolloutPercent: func(string) int { return 50 },
	}).AnyTimes()

	selected := 0
	for i := range 100 {
		workflowID := "workflow-" + strconv.Itoa(i)
		first := isWorkflowHeartbeatNotificationEnabled(shard, "namespace", workflowID)
		require.Equal(t, first, isWorkflowHeartbeatNotificationEnabled(shard, "namespace", workflowID))
		if first {
			selected++
		}
	}
	require.Positive(t, selected)
	require.Less(t, selected, 100)
}

func TestHandleWorkflowHeartbeatNotification(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name          string
		started       bool
		newEvents     []enumspb.EventType
		priorEvents   []enumspb.EventType
		pendingUpdate bool
		bufferedTypes []string
		firstType     string
		nilNamespace  bool
	}{
		{
			name:          "first timer result",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
			bufferedTypes: []string{"TimerFired"},
			firstType:     "TimerFired",
		},
		{
			name:          "previous signal",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			bufferedTypes: []string{"TimerFired"},
		},
		{
			name:          "previous update",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
			pendingUpdate: true,
			bufferedTypes: []string{"TimerFired"},
		},
		{
			name:          "multiple events in one transaction",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED, enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED},
			bufferedTypes: []string{"TimerFired", "ActivityTaskCompleted"},
			firstType:     "TimerFired",
		},
		{
			name:          "unrelated buffered event",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_NEXUS_OPERATION_COMPLETED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED},
			bufferedTypes: []string{"NexusOperationCompleted"},
		},
		{
			name:      "workflow task not started",
			newEvents: []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
		},
		{
			name: "no new buffered events",
		},
		{
			name:         "nil namespace entry",
			started:      true,
			newEvents:    []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
			nilNamespace: true,
		},
		{
			name:      "no eligible event",
			started:   true,
			newEvents: []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(controller)
			if len(tc.newEvents) > 0 {
				mutableState.EXPECT().HasStartedWorkflowTask().Return(tc.started)
			}
			if tc.started && (len(tc.bufferedTypes) > 0 || tc.nilNamespace) {
				mutableState.EXPECT().NumBufferedEvents().Return(len(tc.priorEvents) + len(tc.newEvents))
				namespaceEntry := tests.GlobalNamespaceEntry
				if tc.nilNamespace {
					namespaceEntry = nil
				}
				mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry)
				if len(tc.priorEvents) == 0 || tc.firstType != "" || tc.pendingUpdate {
					mutableState.EXPECT().GetCurrentVersion().Return(int64(1))
				}
			}

			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			workflowContext := &ContextImpl{
				MutableState:   mutableState,
				workflowKey:    definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"),
				metricsHandler: metricsHandler,
				updateRegistry: bufferedWorkflowHeartbeatNotificationUpdateRegistry{
					hasOutgoing: tc.pendingUpdate,
				},
			}
			newEvents := make([]*historypb.HistoryEvent, 0, len(tc.newEvents))
			for _, eventType := range tc.newEvents {
				newEvents = append(newEvents, &historypb.HistoryEvent{EventType: eventType})
			}
			shard := historyi.NewMockShardContext(controller)
			if tc.firstType != "" {
				mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{})
			}
			workflowContext.handleWorkflowHeartbeatNotification(context.Background(), shard, newEvents)

			buffered := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
			require.Len(t, buffered, len(tc.bufferedTypes))
			for i, inputType := range tc.bufferedTypes {
				require.Equal(t, inputType, buffered[i].Tags["event_type"])
				require.Equal(t, tests.GlobalNamespaceEntry.Name().String(), buffered[i].Tags["namespace"])
			}
			first := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name())
			if tc.firstType == "" {
				require.Empty(t, first)
			} else {
				require.Len(t, first, 1)
				require.Equal(t, tc.firstType, first[0].Tags["event_type"])
			}
		})
	}
}

func TestHasPendingUpdateForWorkflowHeartbeatNotification(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name              string
		registry          update.Registry
		version           int64
		wantPendingUpdate bool
	}{
		{
			name:              "current registry has pending update",
			registry:          bufferedWorkflowHeartbeatNotificationUpdateRegistry{hasOutgoing: true},
			version:           1,
			wantPendingUpdate: true,
		},
		{
			name:     "current registry has no pending update",
			registry: bufferedWorkflowHeartbeatNotificationUpdateRegistry{},
			version:  1,
		},
		{
			name:     "stale registry with outgoing update",
			registry: bufferedWorkflowHeartbeatNotificationUpdateRegistry{hasOutgoing: true},
			version:  2,
		},
		{
			name: "no registry",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mutableState := historyi.NewMockMutableState(gomock.NewController(t))
			if tc.registry != nil {
				mutableState.EXPECT().GetCurrentVersion().Return(tc.version)
			}
			workflowContext := &ContextImpl{MutableState: mutableState, updateRegistry: tc.registry}
			require.Equal(t, tc.wantPendingUpdate, workflowContext.hasPendingUpdateForWorkflowHeartbeatNotification())
		})
	}
}
