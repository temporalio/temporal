package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	workerservicepb "go.temporal.io/api/nexusservices/workerservice/v1"
	workerpb "go.temporal.io/api/worker/v1"
	clockspb "go.temporal.io/server/api/clock/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestPrepareWorkflowHeartbeatNotification(t *testing.T) {
	ctrl := gomock.NewController(t)
	ms := historyi.NewMockMutableState(ctrl)
	started := time.Date(2026, 11, 8, 12, 0, 0, 0, time.UTC)
	clock := &clockspb.VectorClock{ClusterId: 1, ShardId: 2, Clock: 3}
	ms.EXPECT().GetStartedWorkflowTask().Return(&historyi.WorkflowTaskInfo{
		ScheduledEventID:       5,
		StartedEventID:         6,
		StartedTime:            started,
		Attempt:                2,
		Version:                9,
		StartedClock:           clock,
		WorkerControlTaskQueue: "control-queue",
	})
	ms.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey("namespace-id", "workflow-id", "run-id"))
	ms.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)

	notification, err := PrepareWorkflowHeartbeatNotification(ms, enumspb.EventType_name[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)])
	require.NoError(t, err)
	require.Equal(t, "control-queue", notification.controlQueue)
	token, err := tasktoken.NewSerializer().Deserialize(notification.taskToken)
	require.NoError(t, err)
	require.Equal(t, "namespace-id", token.GetNamespaceId())
	require.Equal(t, int64(5), token.GetScheduledEventId())
	require.Equal(t, int64(6), token.GetStartedEventId())
	require.True(t, proto.Equal(clock, token.GetClock()))
}

func TestSendWorkflowHeartbeatNotification(t *testing.T) {
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		EnableWorkflowHeartbeatNotification:          func(namespace string) bool { return namespace == "namespace-name" },
		WorkflowHeartbeatNotificationInputTypes:      func(string) []string { return []string{"signal"} },
		WorkflowHeartbeatNotificationDispatchTimeout: func(string) time.Duration { return 500 * time.Millisecond },
	}).AnyTimes()
	delivered := make(chan *matchingservice.DispatchNexusTaskRequest, 1)
	shard.EXPECT().GetMatchingClient().Return(&whnMatchingClient{delivered: delivered}, nil)
	SendWorkflowHeartbeatNotification(context.Background(), shard, &WorkflowHeartbeatNotification{
		namespaceID:   "namespace-id",
		namespaceName: "namespace-name",
		workflowID:    "workflow-id",
		runID:         "run-id",
		controlQueue:  "control-queue",
		taskToken:     []byte("task-token"),
		eventType:     enumspb.EventType_name[int32(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED)],
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
}

func TestSendWorkflowHeartbeatNotificationDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		EnableWorkflowHeartbeatNotification: func(string) bool { return false },
	})
	SendWorkflowHeartbeatNotification(context.Background(), shard, &WorkflowHeartbeatNotification{
		namespaceName: "namespace-name",
	})
}

type whnMatchingClient struct {
	matchingservice.MatchingServiceClient
	delivered chan *matchingservice.DispatchNexusTaskRequest
}

func (c *whnMatchingClient) DispatchNexusTask(
	_ context.Context,
	request *matchingservice.DispatchNexusTaskRequest,
	_ ...grpc.CallOption,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	c.delivered <- request
	return &matchingservice.DispatchNexusTaskResponse{}, nil
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
		{"activity result", enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, true},
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

func TestWorkflowHeartbeatNotificationInputTypeForEvent(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		eventType enumspb.EventType
		want      string
	}{
		{"signal", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED, "signal"},
		{"cancellation", enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCEL_REQUESTED, "cancellation"},
		{"timer", enumspb.EVENT_TYPE_TIMER_FIRED, "timer"},
		{"activity result", enumspb.EVENT_TYPE_ACTIVITY_TASK_FAILED, "activity_result"},
		{"child workflow result", enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_COMPLETED, "child_workflow_result"},
		{"nexus result", enumspb.EVENT_TYPE_NEXUS_OPERATION_TIMED_OUT, "nexus_result"},
		{"activity progress", enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, ""},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, workflowHeartbeatNotificationInputTypeForEvent(tc.eventType))
		})
	}
}

func TestIsWorkflowHeartbeatNotificationInputEnabled(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name      string
		enabled   bool
		allowed   []string
		inputType string
		want      bool
	}{
		{"master flag off", false, nil, "WorkflowExecutionSignaled", false},
		{"empty list disables all", true, nil, "ActivityTaskCompleted", false},
		{"selected signal", true, []string{"signal"}, "WorkflowExecutionSignaled", true},
		{"mixed-case signal", true, []string{"sIgnal"}, "WorkflowExecutionSignaled", true},
		{"unselected timer", true, []string{"signal"}, "TimerFired", false},
		{"selected update", true, []string{"update"}, WorkflowHeartbeatNotificationUpdateAdmittedMetricTag, true},
		{"uppercase update", true, []string{"UPDATE"}, WorkflowHeartbeatNotificationUpdateAdmittedMetricTag, true},
		{"unknown event", true, []string{"signal"}, "Unrecognized", false},
		{"unknown event with empty list", true, nil, "Unrecognized", false},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shard := historyi.NewMockShardContext(gomock.NewController(t))
			shard.EXPECT().GetConfig().Return(&configs.Config{
				EnableWorkflowHeartbeatNotification:     func(string) bool { return tc.enabled },
				WorkflowHeartbeatNotificationInputTypes: func(string) []string { return tc.allowed },
			})
			require.Equal(t, tc.want, isWorkflowHeartbeatNotificationInputEnabled(shard, "namespace", tc.inputType))
		})
	}
}

func TestHandleBufferedInputsForWorkflowHeartbeatNotification(t *testing.T) {
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
			name:          "multiple inputs in one transaction",
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
			name:      "no eligible input",
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
				mutableState.EXPECT().GetStartedWorkflowTask().Return(nil)
			}
			workflowContext.handleBufferedInputsForWorkflowHeartbeatNotification(context.Background(), shard, newEvents)

			buffered := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
			require.Len(t, buffered, len(tc.bufferedTypes))
			for i, inputType := range tc.bufferedTypes {
				require.Equal(t, inputType, buffered[i].Tags["event_type"])
				require.NotContains(t, buffered[i].Tags, "input_type")
				require.Equal(t, tests.GlobalNamespaceEntry.Name().String(), buffered[i].Tags["namespace"])
			}
			first := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name())
			if tc.firstType == "" {
				require.Empty(t, first)
			} else {
				require.Len(t, first, 1)
				require.Equal(t, tc.firstType, first[0].Tags["event_type"])
				require.NotContains(t, first[0].Tags, "input_type")
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
