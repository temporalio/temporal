package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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

func TestEmitWorkflowHeartbeatNotificationBufferedInputMetrics(t *testing.T) {
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
			workflowContext.emitWorkflowHeartbeatNotificationBufferedInputMetrics(newEvents)

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
		persistedUpdates  map[string]*persistencespb.UpdateInfo
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
			name: "no registry with persisted admission",
			persistedUpdates: map[string]*persistencespb.UpdateInfo{
				"update": {Value: &persistencespb.UpdateInfo_Admission{Admission: &persistencespb.UpdateAdmissionInfo{}}},
			},
			wantPendingUpdate: true,
		},
		{
			name:     "stale registry with persisted admission",
			registry: bufferedWorkflowHeartbeatNotificationUpdateRegistry{},
			version:  2,
			persistedUpdates: map[string]*persistencespb.UpdateInfo{
				"update": {Value: &persistencespb.UpdateInfo_Admission{Admission: &persistencespb.UpdateAdmissionInfo{}}},
			},
			wantPendingUpdate: true,
		},
		{
			name:     "stale registry without persisted admission",
			registry: bufferedWorkflowHeartbeatNotificationUpdateRegistry{hasOutgoing: true},
			version:  2,
		},
		{
			name: "no registry or persisted admission",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mutableState := historyi.NewMockMutableState(gomock.NewController(t))
			if tc.registry != nil {
				mutableState.EXPECT().GetCurrentVersion().Return(tc.version)
			}
			if tc.registry == nil || tc.version != 1 {
				mutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{UpdateInfos: tc.persistedUpdates})
			}
			workflowContext := &ContextImpl{MutableState: mutableState, updateRegistry: tc.registry}
			require.Equal(t, tc.wantPendingUpdate, workflowContext.hasPendingUpdateForWorkflowHeartbeatNotification())
		})
	}
}
