package workflow

import (
	"context"
	"slices"
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
	"go.temporal.io/server/service/history/tasks"
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
		name           string
		resp           *matchingservice.DispatchNexusTaskResponse
		err            error
		expectedResult string
		expectedLog    string
	}{
		{
			name:           "worker accepted",
			resp:           whnWorkerResponse(t, workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_ACCEPTED, nil),
			expectedResult: "OUTCOME_ACCEPTED",
		},
		{
			name:           "task token mismatch",
			resp:           whnWorkerResponse(t, workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_TASK_TOKEN_MISMATCH, []byte("current-token")),
			expectedResult: "OUTCOME_TASK_TOKEN_MISMATCH",
			expectedLog:    "WHN: Workflow heartbeat notification task token mismatch",
		},
		{
			name:           "no current task",
			resp:           whnWorkerResponse(t, workerpb.WorkflowHeartbeatNotificationResult_OUTCOME_TASK_TOKEN_MISMATCH, nil),
			expectedResult: "OUTCOME_TASK_TOKEN_MISMATCH",
			expectedLog:    "WHN: Workflow heartbeat notification task token mismatch",
		},
		{
			name: "invalid worker response",
			resp: &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
				Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
					Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{}},
				}}},
			}},
			expectedResult: "invalid_response",
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
			expectedResult: "handler_error:NOT_IMPLEMENTED",
			expectedLog:    "WHN: Worker failed workflow heartbeat notification",
		},
		{
			name:           "dispatch timeout",
			err:            context.DeadlineExceeded,
			expectedResult: "dispatch_timeout",
			expectedLog:    "WHN: Unable to dispatch workflow heartbeat notification",
		},
		{
			name:           "dispatch error",
			err:            serviceerror.NewUnavailable("unavailable"),
			expectedResult: "dispatch_error",
			expectedLog:    "WHN: Unable to dispatch workflow heartbeat notification",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			shard := historyi.NewMockShardContext(ctrl)
			shard.EXPECT().GetConfig().Return(&configs.Config{
				WorkflowHeartbeatNotificationRolloutPercent:  func(string) int { return 100 },
				WorkflowHeartbeatNotificationDispatchTimeout: func(string) time.Duration { return 500 * time.Millisecond },
			}).AnyTimes()
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)
			shard.EXPECT().GetMetricsHandler().Return(metricsHandler).AnyTimes()
			logs := make(chan string, 1)
			logger := log.NewMockLogger(ctrl)
			logger.EXPECT().Error(gomock.Any(), gomock.Any()).Do(func(msg string, _ ...tag.Tag) { logs <- msg }).AnyTimes()
			logger.EXPECT().Info(gomock.Any(), gomock.Any()).Do(func(msg string, _ ...tag.Tag) { logs <- msg }).AnyTimes()
			shard.EXPECT().GetLogger().Return(logger).AnyTimes()
			engine := historyi.NewMockEngine(ctrl)
			shard.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
			engine.EXPECT().NotifyNewTasks(gomock.Any()).Do(func(batch map[tasks.Category][]tasks.Task) {
				for _, task := range batch[tasks.CategoryMemoryImmediate] {
					require.Equal(t, 500*time.Millisecond, task.(*tasks.MemoryImmediateTask).QueueTimeout)
					task.(*tasks.MemoryImmediateTask).Run(context.Background())
				}
			})
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
			if tc.expectedLog != "" {
				require.Equal(t, tc.expectedLog, <-logs)
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

func whnWorkerResponse(
	t *testing.T,
	outcome workerpb.WorkflowHeartbeatNotificationResult_Outcome,
	currentTaskToken []byte,
) *matchingservice.DispatchNexusTaskResponse {
	data, err := proto.Marshal(&workerservicepb.ExecuteCommandsResponse{Results: []*workerpb.WorkerCommandResult{{
		Type: &workerpb.WorkerCommandResult_WorkflowHeartbeatNotification{
			WorkflowHeartbeatNotification: &workerpb.WorkflowHeartbeatNotificationResult{Outcome: outcome, CurrentTaskToken: currentTaskToken},
		},
	}}})
	require.NoError(t, err)
	return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
		Response: &nexuspb.Response{Variant: &nexuspb.Response_StartOperation{StartOperation: &nexuspb.StartOperationResponse{
			Variant: &nexuspb.StartOperationResponse_SyncSuccess{SyncSuccess: &nexuspb.StartOperationResponse_Sync{
				Payload: &commonpb.Payload{Data: data},
			}},
		}}},
	}}
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

func TestHasBufferedWorkflowHeartbeatNotificationInput(t *testing.T) {
	t.Parallel()
	input := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_TIMER_FIRED}
	newInput := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_TIMER_FIRED}
	nonInput := &historypb.HistoryEvent{EventType: enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED}
	newEvents := make([]*historypb.HistoryEvent, 5)
	for i := range newEvents {
		eventType := enumspb.EVENT_TYPE_TIMER_FIRED
		if i%2 == 0 {
			eventType = enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED
		}
		newEvents[i] = &historypb.HistoryEvent{EventType: eventType}
	}
	for _, tc := range []struct {
		name      string
		buffered  []*historypb.HistoryEvent
		newEvents []*historypb.HistoryEvent
		found     bool
		checked   int
	}{
		{name: "empty buffer"},
		{name: "prior input", buffered: []*historypb.HistoryEvent{input, nonInput}, found: true, checked: 1},
		{name: "only non-input events", buffered: []*historypb.HistoryEvent{nonInput}, checked: 1},
		{name: "only new input", buffered: []*historypb.HistoryEvent{newInput}, newEvents: []*historypb.HistoryEvent{newInput}, checked: 1},
		{name: "prior and new inputs of the same type", buffered: []*historypb.HistoryEvent{newInput, input}, newEvents: []*historypb.HistoryEvent{newInput}, found: true, checked: 2},
		{name: "only new events", buffered: newEvents, newEvents: newEvents, checked: 5},
		{name: "new events before prior input", buffered: append(slices.Clone(newEvents), input), newEvents: newEvents, found: true, checked: 6},
		{name: "new events before input at prior scan limit", buffered: append(append(slices.Clone(newEvents), slices.Repeat([]*historypb.HistoryEvent{nonInput}, 4)...), input), newEvents: newEvents, found: true, checked: 10},
		{name: "new events before input beyond prior scan limit", buffered: append(append(slices.Clone(newEvents), slices.Repeat([]*historypb.HistoryEvent{nonInput}, 5)...), input), newEvents: newEvents, checked: 10},
		{name: "input at scan limit", buffered: append(slices.Repeat([]*historypb.HistoryEvent{nonInput}, 4), input), found: true, checked: 5},
		{name: "input beyond scan limit", buffered: append(slices.Repeat([]*historypb.HistoryEvent{nonInput}, 5), input), checked: 5},
		{name: "non-input events beyond scan limit", buffered: slices.Repeat([]*historypb.HistoryEvent{nonInput}, 6), checked: 5},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ms := historyi.NewMockMutableState(gomock.NewController(t))
			checked := 0
			ms.EXPECT().HasAnyBufferedEvent(gomock.Any()).DoAndReturn(func(filter func(*historypb.HistoryEvent) bool) bool {
				return slices.ContainsFunc(tc.buffered, func(event *historypb.HistoryEvent) bool {
					checked++
					return filter(event)
				})
			})
			require.Equal(t, tc.found, HasBufferedWorkflowHeartbeatNotificationInput(ms, tc.newEvents))
			require.Equal(t, tc.checked, checked)
		})
	}
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
			firstType:     "NexusOperationCompleted",
		},
		{
			name:          "activity start before signal",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED},
			bufferedTypes: []string{"WorkflowExecutionSignaled"},
			firstType:     "WorkflowExecutionSignaled",
		},
		{
			name:          "activity start before completion",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED},
			bufferedTypes: []string{"ActivityTaskCompleted"},
			firstType:     "ActivityTaskCompleted",
		},
		{
			name:          "activity start with pending update",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED},
			pendingUpdate: true,
			bufferedTypes: []string{"WorkflowExecutionSignaled"},
		},
		{
			name:          "prior input of the same type",
			started:       true,
			newEvents:     []enumspb.EventType{enumspb.EVENT_TYPE_TIMER_FIRED},
			priorEvents:   []enumspb.EventType{enumspb.EVENT_TYPE_ACTIVITY_TASK_STARTED, enumspb.EVENT_TYPE_TIMER_FIRED},
			bufferedTypes: []string{"TimerFired"},
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
			newEvents := make([]*historypb.HistoryEvent, 0, len(tc.newEvents))
			for _, eventType := range tc.newEvents {
				newEvents = append(newEvents, &historypb.HistoryEvent{EventType: eventType})
			}
			var bufferedEvents []*historypb.HistoryEvent
			for _, eventType := range tc.priorEvents {
				bufferedEvents = append(bufferedEvents, &historypb.HistoryEvent{EventType: eventType})
			}
			bufferedEvents = append(bufferedEvents, newEvents...)
			controller := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(controller)
			if len(tc.newEvents) > 0 {
				mutableState.EXPECT().HasStartedWorkflowTask().Return(tc.started)
			}
			if tc.started && (len(tc.bufferedTypes) > 0 || tc.nilNamespace) {
				mutableState.EXPECT().HasAnyBufferedEvent(gomock.Any()).DoAndReturn(func(filter func(*historypb.HistoryEvent) bool) bool {
					return slices.ContainsFunc(bufferedEvents, filter)
				})
				namespaceEntry := tests.GlobalNamespaceEntry
				if tc.nilNamespace {
					namespaceEntry = nil
				}
				mutableState.EXPECT().GetNamespaceEntry().Return(namespaceEntry)
				if !slices.ContainsFunc(tc.priorEvents, IsWorkflowHeartbeatNotificationInputType) {
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

func TestWorkflowHeartbeatNotificationQueueContext(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		WorkflowHeartbeatNotificationRolloutPercent:  func(string) int { return 100 },
		WorkflowHeartbeatNotificationDispatchTimeout: func(string) time.Duration { return time.Minute },
	}).AnyTimes()
	shard.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	shard.EXPECT().GetLogger().Return(log.NewNoopLogger()).AnyTimes()
	engine := historyi.NewMockEngine(ctrl)
	shard.EXPECT().GetEngine(gomock.Any()).Return(engine, nil)
	var task *tasks.MemoryImmediateTask
	engine.EXPECT().NotifyNewTasks(gomock.Any()).Do(func(batch map[tasks.Category][]tasks.Task) {
		task = batch[tasks.CategoryMemoryImmediate][0].(*tasks.MemoryImmediateTask)
	})
	started := make(chan context.Context, 1)
	shard.EXPECT().GetMatchingClient().Return(&whnBlockingMatchingClient{started: started}, nil)
	type contextKey struct{}
	key := contextKey{}
	requestCtx, requestCancel := context.WithCancel(context.WithValue(t.Context(), key, "request metadata"))
	requestCancel()
	SendWorkflowHeartbeatNotification(requestCtx, shard, &WorkflowHeartbeatNotification{namespaceName: "namespace"})
	require.NotNil(t, task)
	queueCtx, queueCancel := context.WithCancel(t.Context())
	defer queueCancel()
	finished := make(chan struct{}, 1)
	go func() { task.Run(queueCtx); finished <- struct{}{} }()
	dispatchCtx := await.Rcv(t, started)
	require.NoError(t, dispatchCtx.Err())
	require.Equal(t, "request metadata", dispatchCtx.Value(key))
	queueCancel()
	await.Rcv(t, finished)
	require.ErrorIs(t, dispatchCtx.Err(), context.Canceled)
}

type whnBlockingMatchingClient struct {
	matchingservice.MatchingServiceClient
	started chan context.Context
}

func (c *whnBlockingMatchingClient) DispatchNexusTask(ctx context.Context, _ *matchingservice.DispatchNexusTaskRequest, _ ...grpc.CallOption) (*matchingservice.DispatchNexusTaskResponse, error) {
	c.started <- ctx
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestWorkflowHeartbeatNotificationUnavailableEngine(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	shard := historyi.NewMockShardContext(ctrl)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		WorkflowHeartbeatNotificationRolloutPercent: func(string) int { return 100 },
	})
	shard.EXPECT().GetEngine(gomock.Any()).Return(nil, context.Canceled)
	shard.EXPECT().GetLogger().Return(log.NewNoopLogger())
	SendWorkflowHeartbeatNotification(t.Context(), shard, &WorkflowHeartbeatNotification{namespaceName: "namespace"})
}
