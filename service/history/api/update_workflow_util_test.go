package api

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/historybuilder"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/update"
	"go.uber.org/mock/gomock"
)

type bufferedInputUpdateRegistry struct {
	update.Registry
	hasOutgoing bool
}

func (r bufferedInputUpdateRegistry) HasOutgoingMessages(bool) bool {
	return r.hasOutgoing
}

func TestUpdateWorkflowWithNewBufferedInputMetrics(t *testing.T) {
	persistErr := errors.New("persistence failed")
	testCases := []struct {
		name                  string
		bufferedEvents        []enumspb.EventType
		started               bool
		pendingUpdate         bool
		persistErr            error
		expectedBufferedCount int
		expectedFirstCount    int
	}{
		{
			name:                  "first buffered input",
			bufferedEvents:        []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			started:               true,
			expectedBufferedCount: 1,
			expectedFirstCount:    1,
		},
		{
			name:                  "update already waiting",
			bufferedEvents:        []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			started:               true,
			pendingUpdate:         true,
			expectedBufferedCount: 1,
		},
		{
			name: "another input already buffered",
			bufferedEvents: []enumspb.EventType{
				enumspb.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			},
			started:               true,
			expectedBufferedCount: 1,
		},
		{
			name: "unrelated event already buffered",
			bufferedEvents: []enumspb.EventType{
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_PAUSED,
				enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED,
			},
			started:               true,
			expectedBufferedCount: 1,
			expectedFirstCount:    1,
		},
		{
			name:           "workflow task not started",
			bufferedEvents: []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
		},
		{
			name:           "persistence failure",
			bufferedEvents: []enumspb.EventType{enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_SIGNALED},
			started:        true,
			persistErr:     persistErr,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			mutableState := historyi.NewMockMutableState(controller)
			workflowContext := historyi.NewMockWorkflowContext(controller)
			shard := historyi.NewMockShardContext(controller)
			metricsHandler := metricstest.NewCaptureHandler()
			capture := metricsHandler.StartCapture()
			defer metricsHandler.StopCapture(capture)

			mutableState.EXPECT().HasPendingWorkflowTask().Return(true).Times(2)
			mutableState.EXPECT().HasStartedWorkflowTask().Return(tc.started)
			if tc.started {
				mutableState.EXPECT().HasAnyBufferedEvent(gomock.Any()).DoAndReturn(func(filter historybuilder.BufferedEventFilter) bool {
					for _, eventType := range tc.bufferedEvents {
						if filter(&historypb.HistoryEvent{EventType: eventType}) {
							return true
						}
					}
					return false
				})
				if tc.expectedFirstCount == 1 || tc.pendingUpdate || tc.persistErr != nil {
					workflowContext.EXPECT().UpdateRegistry(gomock.Any()).Return(bufferedInputUpdateRegistry{hasOutgoing: tc.pendingUpdate})
				}
			}
			workflowContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), shard).Return(tc.persistErr)
			if tc.expectedBufferedCount != 0 {
				shard.EXPECT().GetMetricsHandler().Return(metricsHandler)
				mutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
			}

			err := UpdateWorkflowWithNew(
				shard,
				context.Background(),
				NewWorkflowLease(workflowContext, func(error) {}, mutableState),
				func(WorkflowLease) (*UpdateWorkflowAction, error) {
					return &UpdateWorkflowAction{
						CreateWorkflowTask: true,
						InputType:          WHNInputTypeSignal,
					}, nil
				},
				nil,
			)
			if tc.persistErr != nil {
				require.ErrorIs(t, err, tc.persistErr)
			} else {
				require.NoError(t, err)
			}
			buffered := capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
			require.Len(t, buffered, tc.expectedBufferedCount)
			if len(buffered) > 0 {
				require.Equal(t, tests.GlobalNamespaceEntry.Name().String(), buffered[0].Tags[metrics.NamespaceTag("").Key])
				require.Equal(t, string(WHNInputTypeSignal), buffered[0].Tags[metrics.StringTag("input_type", "").Key])
			}
			require.Len(t, capture.SnapshotMetric(metrics.WorkflowHeartbeatNotificationFirstBufferedInput.Name()), tc.expectedFirstCount)
		})
	}
}
