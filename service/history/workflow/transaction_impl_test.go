package workflow

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/notification"
	"go.uber.org/mock/gomock"
)

func TestNotifyFastForwardUpdate(t *testing.T) {
	const (
		namespaceID = "namespace-id"
		workflowID  = "workflow-id"
	)
	archetypeID := chasm.WorkflowArchetypeID
	currentVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 9}
	staleVT := &persistencespb.VersionedTransition{NamespaceFailoverVersion: 1, TransitionCount: 4}

	newExecutionInfo := func(
		ffVT *persistencespb.VersionedTransition,
		transitionHistory []*persistencespb.VersionedTransition,
	) *persistencespb.WorkflowExecutionInfo {
		return &persistencespb.WorkflowExecutionInfo{
			NamespaceId:       namespaceID,
			WorkflowId:        workflowID,
			TransitionHistory: transitionHistory,
			TimeSkippingInfo: &persistencespb.TimeSkippingInfo{
				Config:          &commonpb.TimeSkippingConfig{Enabled: true},
				FastForwardInfo: &persistencespb.FastForwardInfo{HasReached: false},
				FastForwardInfoLastUpdateVersionedTransition: ffVT,
			},
		}
	}
	runningState := &persistencespb.WorkflowExecutionState{
		State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
	}

	for _, tc := range []struct {
		name           string
		executionInfo  *persistencespb.WorkflowExecutionInfo
		executionState *persistencespb.WorkflowExecutionState
		wantNotify     bool
		wantCompleted  bool
	}{
		{
			name: "NoTimeSkippingInfo",
			executionInfo: &persistencespb.WorkflowExecutionInfo{
				NamespaceId: namespaceID,
				WorkflowId:  workflowID,
			},
			executionState: runningState,
		},
		{
			name:           "StampedThisTransaction",
			executionInfo:  newExecutionInfo(currentVT, []*persistencespb.VersionedTransition{staleVT, currentVT}),
			executionState: runningState,
			wantNotify:     true,
		},
		{
			name:           "StampedInEarlierTransaction",
			executionInfo:  newExecutionInfo(staleVT, []*persistencespb.VersionedTransition{staleVT, currentVT}),
			executionState: runningState,
		},
		{
			name:          "StaleStampButRunChainEnded",
			executionInfo: newExecutionInfo(staleVT, []*persistencespb.VersionedTransition{staleVT, currentVT}),
			executionState: &persistencespb.WorkflowExecutionState{
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
			wantNotify:    true,
			wantCompleted: true,
		},
		{
			name: "StaleStampAndRunChainContinues",
			executionInfo: func() *persistencespb.WorkflowExecutionInfo {
				ei := newExecutionInfo(staleVT, []*persistencespb.VersionedTransition{staleVT, currentVT})
				ei.NewExecutionRunId = "next-run-id"
				return ei
			}(),
			executionState: &persistencespb.WorkflowExecutionState{
				State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			},
		},
		{
			// Time skipping requires transition history; with none there is no versioned
			// transition to match the stamp against, so nothing is delivered.
			name:           "NoTransitionHistorySuppresses",
			executionInfo:  newExecutionInfo(staleVT, nil),
			executionState: runningState,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			engine := interfaces.NewMockEngine(controller)

			if tc.wantNotify {
				engine.EXPECT().NotifyFastForwardUpdate(
					notification.NewTimeSkippingNotificationKey(namespaceID, workflowID, archetypeID),
					gomock.Any(),
				).Do(func(_ any, notif *notification.TimeSkippingFastForwardNotification) {
					require.Equal(t, tc.wantCompleted, notif.WorkflowExecutionCompleted)
					require.NotSame(t, tc.executionInfo.GetTimeSkippingInfo(), notif.GetTimeSkippingInfo())
				}).Times(1)
			}

			notifyFastForwardUpdate(engine, archetypeID, tc.executionInfo, tc.executionState)
		})
	}
}
