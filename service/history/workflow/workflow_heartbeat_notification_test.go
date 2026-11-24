package workflow

import (
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

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
				shard.EXPECT().GetConfig().Return(&configs.Config{
					WorkflowHeartbeatNotificationRolloutPercent: func(string) int { return 0 },
				})
			}
			workflowContext.handleWorkflowHeartbeatNotification(shard, newEvents)

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
