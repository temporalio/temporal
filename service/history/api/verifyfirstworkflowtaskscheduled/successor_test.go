package verifyfirstworkflowtaskscheduled

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.uber.org/mock/gomock"
)

func TestVerifyFirstWorkflowTaskScheduled_CurrentRunFallback(t *testing.T) {
	for _, tc := range []struct {
		name, firstRunID, currentRunID string
		firstRunErr, lookupErr         error
		original                       bool
	}{
		{name: "retained successor", firstRunID: "first", currentRunID: "successor"},
		{name: "reused workflow ID", firstRunID: "different", currentRunID: "unrelated"},
		{name: "first run ID lookup error", firstRunErr: serviceerror.NewUnavailable("history unavailable")},
		{name: "current missing", lookupErr: serviceerror.NewNotFound("missing")},
		{name: "current lookup unavailable", lookupErr: serviceerror.NewUnavailable("unavailable")},
		{name: "original arrived during lookup", firstRunID: "first", currentRunID: "first", original: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			checker := api.NewMockWorkflowConsistencyChecker(ctrl)
			checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), definition.NewWorkflowKey("namespace", "child", "first"), locks.PriorityLow).Return(nil, serviceerror.NewNotFound("retained out"))
			if tc.lookupErr != nil {
				checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), definition.NewWorkflowKey("namespace", "child", ""), locks.PriorityLow).Return(nil, tc.lookupErr)
			} else {
				ms := historyi.NewMockMutableState(ctrl)
				ms.EXPECT().GetFirstRunID(gomock.Any()).Return(tc.firstRunID, tc.firstRunErr)
				if tc.firstRunErr == nil && tc.firstRunID == "first" {
					ms.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{RunId: tc.currentRunID})
				}
				if tc.original {
					ms.EXPECT().IsWorkflowExecutionRunning().Return(true)
					ms.EXPECT().HadOrHasWorkflowTask().Return(false)
					ms.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{VersionHistories: &historyspb.VersionHistories{}})
				}
				lease := ndc.NewMockWorkflow(ctrl)
				lease.EXPECT().GetMutableState().Return(ms)
				lease.EXPECT().GetReleaseFn().Return(func(error) {})
				checker.EXPECT().GetWorkflowLease(gomock.Any(), gomock.Any(), definition.NewWorkflowKey("namespace", "child", ""), locks.PriorityLow).Return(lease, nil)
			}
			_, _, missing, err := verifyFirstWorkflowTaskScheduled(t.Context(), &historyservice.VerifyFirstWorkflowTaskScheduledRequest{NamespaceId: "namespace", WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: "child", RunId: "first"}}, checker)
			switch {
			case tc.lookupErr != nil:
				require.Same(t, tc.lookupErr, err)
			case tc.firstRunErr != nil:
				require.Same(t, tc.firstRunErr, err)
			case tc.original:
				require.ErrorIs(t, err, consts.ErrWorkflowNotReady)
				require.True(t, missing)
			case tc.firstRunID != "first":
				require.ErrorAs(t, err, new(*serviceerror.NotFound))
			default:
				require.NoError(t, err)
				require.False(t, missing)
			}
		})
	}
}
