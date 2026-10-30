package verifychildworkflowcompletionrecorded

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestVerifyChildExecution_InitiatedEventNotOnCurrentBranch(t *testing.T) {
	const (
		initiatedEventID       = int64(10)
		currentBranchVersion   = int64(1)
		alternateBranchVersion = int64(2)
	)

	versionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{
			versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
				versionhistory.NewVersionHistoryItem(initiatedEventID, currentBranchVersion),
			}),
			versionhistory.NewVersionHistory(nil, []*historyspb.VersionHistoryItem{
				versionhistory.NewVersionHistoryItem(initiatedEventID, alternateBranchVersion),
			}),
		},
	}
	executionState := &persistencespb.WorkflowExecutionState{
		State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		VersionHistories: versionHistories,
	}

	controller := gomock.NewController(t)
	mutableState := historyi.NewMockMutableState(controller)
	mutableState.EXPECT().GetExecutionState().Return(executionState)
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	mutableState.EXPECT().GetExecutionInfo().Return(executionInfo)

	workflowLease := ndc.NewMockWorkflow(controller)
	workflowLease.EXPECT().GetMutableState().Return(mutableState)
	workflowLease.EXPECT().GetReleaseFn().Return(func(error) {})

	workflowConsistencyChecker := api.NewMockWorkflowConsistencyChecker(controller)
	workflowConsistencyChecker.EXPECT().GetWorkflowLease(
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
		gomock.Any(),
	).Return(workflowLease, nil)

	request := &historyservice.VerifyChildExecutionCompletionRecordedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: tests.WorkflowID,
			RunId:      tests.RunID,
		},
		ParentInitiatedId:      initiatedEventID,
		ParentInitiatedVersion: alternateBranchVersion,
		ChildExecution: &commonpb.WorkflowExecution{
			WorkflowId: "child-workflow-id",
			RunId:      "child-run-id",
		},
	}

	versionedTransition, returnedVersionHistories, parentWorkflowState, err := verifyChildExecution(
		t.Context(),
		workflowConsistencyChecker,
		request,
	)
	require.NoError(t, err)
	require.Nil(t, versionedTransition)
	require.Nil(t, returnedVersionHistories)
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING.String(), parentWorkflowState)
}
