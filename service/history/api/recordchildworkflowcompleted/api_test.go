package recordchildworkflowcompleted

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// tests that the child execution completed request is forwarded to the new parent in case of resets.
func Test_Recordchildworkflowcompleted_WithForwards(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFID := uuid.NewString()
	paretntWFID := uuid.NewString()
	oldParentRunID := uuid.NewString()
	newParentRunID := uuid.NewString()
	oldParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, oldParentRunID)
	newParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, newParentRunID)
	oldParentExecutionInfo := &persistencespb.WorkflowExecutionInfo{
		ResetRunId: newParentRunID, // link the old parent to the new parent.
	}

	// The request will be sent to the old parent.
	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			RunId:      oldParentRunID,
			WorkflowId: paretntWFID,
		},
		ChildExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mockRegistery := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(testNamespace, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := historyi.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo)

	newParentMutableState := historyi.NewMockMutableState(ctrl)
	newParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	newParentMutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
	newParentMutableState.EXPECT().GetNextEventID().Return(int64(10))
	newParentMutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(anyArg, anyArg, anyArg).Return(nil, nil)
	childExecutionInfo := &persistencespb.ChildExecutionInfo{
		StartedEventId:    int64(10), // indicate that the started event is already recorded.
		StartedWorkflowId: childWFID,
	}
	newParentMutableState.EXPECT().GetChildExecutionInfo(anyArg).Return(childExecutionInfo, true)
	newParentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	newParentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(anyArg, anyArg).Return(nil, nil)

	mockWFContext := historyi.NewMockWorkflowContext(ctrl)
	mockWFContext.EXPECT().UpdateWorkflowExecutionAsActive(anyArg, anyArg).Return(nil)

	oldParentWFLease := ndc.NewMockWorkflow(ctrl)
	oldParentWFLease.EXPECT().GetMutableState().Return(oldParentMutableState) // old parent's mutable state is accesses just once.
	oldParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease := ndc.NewMockWorkflow(ctrl)
	newParentWFLease.EXPECT().GetMutableState().Return(newParentMutableState).AnyTimes() // new parent's mutable state would be accessed many times.
	newParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease.EXPECT().GetContext().Return(mockWFContext)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, oldParentWFKey, anyArg).Return(oldParentWFLease, nil)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, newParentWFKey, anyArg).Return(newParentWFLease, nil)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, newParentRunID, request.ParentExecution.RunId) // the request should be modified to point to the new parent.
}

// tests that we break out of the loop after max redirect attempts.
func Test_Recordchildworkflowcompleted_WithInfiniteForwards(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFID := uuid.NewString()
	paretntWFID := uuid.NewString()
	oldParentRunID := uuid.NewString()
	oldParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, oldParentRunID)
	oldParentExecutionInfo := &persistencespb.WorkflowExecutionInfo{
		ResetRunId: oldParentRunID, // link to self causing an infinite loop.
	}

	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			RunId:      oldParentRunID,
			WorkflowId: paretntWFID,
		},
		ChildExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mockRegistery := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: testNamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(testNamespace, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := historyi.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false).Times(maxResetRedirectCount + 1)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo).Times(maxResetRedirectCount + 1)

	oldParentWFLease := ndc.NewMockWorkflow(ctrl)
	oldParentWFLease.EXPECT().GetMutableState().Return(oldParentMutableState).Times(maxResetRedirectCount + 1)
	oldParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {}).Times(maxResetRedirectCount + 1)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, oldParentWFKey, anyArg).Return(oldParentWFLease, nil).Times(maxResetRedirectCount + 1)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.ErrorIs(t, err, consts.ErrResetRedirectLimitReached)
	require.Nil(t, resp)
}

// Retention can delete the parent run a child recorded after reset. The completion is delivered to the
// current run when that run still has the same pending child, and rejected when it does not.
func TestRecordChildWorkflowCompletedRecordedRunNotFound(t *testing.T) {
	const (
		childWorkflowID  = "child-wf"
		childFirstRunID  = "child-first-run"
		parentWorkflowID = "parent"
		oldRunID         = "old-parent-run"
		currentRunID     = "current-parent-run"
	)
	recordedMissing := serviceerror.NewNotFound("workflow execution not found for workflow ID \"parent\" and run ID \"old-parent-run\"")
	currentMissing := serviceerror.NewNotFound("workflow not found for ID: parent")
	require.True(t, recordedParentRunMissing(recordedMissing))
	require.False(t, recordedParentRunMissing(consts.ErrWorkflowCompleted))
	require.False(t, recordedParentRunMissing(consts.ErrChildExecutionNotFound))
	require.False(t, recordedParentRunMissing(nil))

	for _, tc := range []struct {
		name          string
		recordedRunID string
		lookupCurrent bool
		currentRunID  string
		currentErr    error
		pendingChild  *persistencespb.ChildExecutionInfo
		recordChild   bool
		wantErr       error
		wantSameErr   error
	}{
		{
			name:          "current run has the pending child",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			pendingChild: &persistencespb.ChildExecutionInfo{
				StartedEventId:    10,
				StartedWorkflowId: childWorkflowID,
				StartedRunId:      childFirstRunID,
			},
			recordChild: true,
		},
		{
			name:          "current run has no matching pending child",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			wantErr:       consts.ErrChildExecutionNotFound,
		},
		{
			name:          "current run pending child started run does not match",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			pendingChild: &persistencespb.ChildExecutionInfo{
				StartedEventId:    10,
				StartedWorkflowId: childWorkflowID,
				StartedRunId:      "some-other-child-run",
			},
			wantErr: consts.ErrChildExecutionNotFound,
		},
		{
			name:          "current run is the recorded run",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  oldRunID,
			wantSameErr:   recordedMissing,
		},
		{
			name:          "current run is also missing",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentErr:    currentMissing,
			wantSameErr:   currentMissing,
		},
		{
			name:          "empty parent run id",
			recordedRunID: "",
			wantSameErr:   recordedMissing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			request := &historyservice.RecordChildExecutionCompletedRequest{
				NamespaceId: tests.NamespaceID.String(),
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: parentWorkflowID,
					RunId:      tc.recordedRunID,
				},
				ChildExecution:           &commonpb.WorkflowExecution{WorkflowId: childWorkflowID},
				ChildFirstExecutionRunId: childFirstRunID,
				CompletionEvent: &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				},
			}
			checker := api.NewMockWorkflowConsistencyChecker(ctrl)
			checker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(
				gomock.Any(), gomock.Any(), gomock.Any(),
				definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, tc.recordedRunID),
				gomock.Any(),
			).Return(nil, recordedMissing)
			if tc.lookupCurrent {
				checker.EXPECT().GetCurrentWorkflowRunID(
					gomock.Any(), tests.NamespaceID.String(), parentWorkflowID, locks.PriorityHigh,
				).Return(tc.currentRunID, tc.currentErr)
			}
			if tc.lookupCurrent && tc.currentErr == nil && tc.currentRunID != tc.recordedRunID {
				checker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(
					gomock.Any(), gomock.Any(), gomock.Any(),
					definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, tc.currentRunID),
					gomock.Any(),
				).Return(parentCompletionLease(ctrl, tc.pendingChild, tc.recordChild), nil)
			}

			resp, err := Invoke(t.Context(), request, activeShard(t, ctrl), checker)
			switch {
			case tc.recordChild:
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, tc.currentRunID, request.ParentExecution.GetRunId())
			case tc.wantErr != nil:
				require.ErrorIs(t, err, tc.wantErr)
				require.Nil(t, resp)
			default:
				require.Same(t, tc.wantSameErr, err)
				require.Nil(t, resp)
				require.Equal(t, tc.recordedRunID, request.ParentExecution.GetRunId())
			}
		})
	}
}

func activeShard(t *testing.T, ctrl *gomock.Controller) historyi.ShardContext {
	t.Helper()
	mockRegistry := namespace.NewMockRegistry(ctrl)
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: tests.NamespaceID.String()},
		Config: &persistencespb.NamespaceConfig{},
	}
	testNamespace, err := namespace.FromPersistentState(detail, factory(detail))
	require.NoError(t, err)
	mockRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(testNamespace, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := historyi.NewMockShardContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistry)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)
	return shardContext
}

func parentCompletionLease(ctrl *gomock.Controller, child *persistencespb.ChildExecutionInfo, recordCompletion bool) *ndc.MockWorkflow {
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	mutableState.EXPECT().GetNextEventID().Return(int64(10))
	if child == nil {
		mutableState.EXPECT().GetChildExecutionInfo(gomock.Any()).Return(nil, false)
	} else {
		mutableState.EXPECT().GetChildExecutionInfo(gomock.Any()).Return(child, true)
	}
	if recordCompletion {
		mutableState.EXPECT().IsWorkflowExecutionStatusPaused().Return(false)
		mutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, nil)
		mutableState.EXPECT().HasPendingWorkflowTask().Return(false)
		mutableState.EXPECT().AddWorkflowTaskScheduledEvent(gomock.Any(), gomock.Any()).Return(nil, nil)
	}

	lease := ndc.NewMockWorkflow(ctrl)
	lease.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	lease.EXPECT().GetReleaseFn().Return(func(error) {})
	if recordCompletion {
		wfContext := historyi.NewMockWorkflowContext(ctrl)
		wfContext.EXPECT().UpdateWorkflowExecutionAsActive(gomock.Any(), gomock.Any()).Return(nil)
		lease.EXPECT().GetContext().Return(wfContext)
	}
	return lease
}
