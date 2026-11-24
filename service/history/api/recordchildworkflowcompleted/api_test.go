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
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
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
// current run when that run's pending child has the same initiated event, child workflow ID, and first run ID.
// A later execution that only reused the workflow ID does not get the completion.
func TestRecordChildWorkflowCompletedRecordedRunNotFound(t *testing.T) {
	const (
		childWorkflowID  = "child-wf"
		childFirstRunID  = "child-first-run"
		parentWorkflowID = "parent"
		oldRunID         = "old-parent-run"
		currentRunID     = "current-parent-run"
		initiatedEventID = int64(7)
	)
	recordedMissing := serviceerror.NewNotFound("workflow execution not found for workflow ID \"parent\" and run ID \"old-parent-run\"")
	currentMissing := serviceerror.NewNotFound("workflow not found for ID: parent")
	unrelatedNotFound := serviceerror.NewNotFound("history event not found")
	emptyChildFirstRunID := ""
	require.True(t, persistence.IsWorkflowExecutionNotFound(recordedMissing, parentWorkflowID, oldRunID))
	require.False(t, persistence.IsWorkflowExecutionNotFound(recordedMissing, parentWorkflowID, currentRunID))
	require.False(t, persistence.IsWorkflowExecutionNotFound(unrelatedNotFound, parentWorkflowID, oldRunID))
	require.False(t, persistence.IsWorkflowExecutionNotFound(consts.ErrWorkflowCompleted, parentWorkflowID, oldRunID))
	require.False(t, persistence.IsWorkflowExecutionNotFound(consts.ErrChildExecutionNotFound, parentWorkflowID, oldRunID))
	require.False(t, persistence.IsWorkflowExecutionNotFound(nil, parentWorkflowID, oldRunID))
	require.NotErrorIs(t, unrelatedNotFound, errRecordedParentRunMissing)
	require.ErrorIs(t, &recordedParentRunMissingError{err: recordedMissing}, errRecordedParentRunMissing)

	for _, tc := range []struct {
		name            string
		recordedRunID   string
		lookupCurrent   bool
		currentRunID    string
		currentErr      error
		loadErr         error
		childFirstRunID *string
		pendingChild    *persistencespb.ChildExecutionInfo
		recordChild     bool
		wantSameErr     error
	}{
		{
			name:          "current run has the pending child",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			pendingChild: &persistencespb.ChildExecutionInfo{
				InitiatedEventId:  initiatedEventID,
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
			wantSameErr:   recordedMissing,
		},
		{
			name:          "reused workflow ID is outside the reset chain",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			pendingChild: &persistencespb.ChildExecutionInfo{
				InitiatedEventId:  initiatedEventID,
				StartedEventId:    10,
				StartedWorkflowId: childWorkflowID,
				StartedRunId:      "unrelated-child-first-run",
			},
			wantSameErr: recordedMissing,
		},
		{
			name:          "look-alike pending child has an empty first run id",
			recordedRunID: oldRunID,
			lookupCurrent: true,
			currentRunID:  currentRunID,
			pendingChild: &persistencespb.ChildExecutionInfo{
				InitiatedEventId:  initiatedEventID,
				StartedEventId:    common.EmptyEventID,
				StartedWorkflowId: childWorkflowID,
			},
			wantSameErr: recordedMissing,
		},
		{
			name:            "empty request child first run id does not fall back",
			recordedRunID:   oldRunID,
			childFirstRunID: &emptyChildFirstRunID,
			wantSameErr:     recordedMissing,
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
			name:          "unrelated not found does not fall back",
			recordedRunID: oldRunID,
			loadErr:       unrelatedNotFound,
			wantSameErr:   unrelatedNotFound,
		},
		{
			name:          "empty parent run id",
			recordedRunID: "",
			wantSameErr:   recordedMissing,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			firstRunID := childFirstRunID
			if tc.childFirstRunID != nil {
				firstRunID = *tc.childFirstRunID
			}
			loadErr := recordedMissing
			if tc.loadErr != nil {
				loadErr = tc.loadErr
			}
			request := &historyservice.RecordChildExecutionCompletedRequest{
				NamespaceId:       tests.NamespaceID.String(),
				ParentInitiatedId: initiatedEventID,
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: parentWorkflowID,
					RunId:      tc.recordedRunID,
				},
				ChildExecution:           &commonpb.WorkflowExecution{WorkflowId: childWorkflowID},
				ChildFirstExecutionRunId: firstRunID,
				CompletionEvent: &historypb.HistoryEvent{
					EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				},
			}
			checker := api.NewMockWorkflowConsistencyChecker(ctrl)
			checker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(
				gomock.Any(), gomock.Any(), gomock.Any(),
				definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, tc.recordedRunID),
				gomock.Any(),
			).Return(nil, loadErr)
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
				).Return(parentCompletionLease(ctrl, initiatedEventID, tc.pendingChild, tc.recordChild, nil), nil)
			}

			resp, err := Invoke(t.Context(), request, activeShard(t, ctrl), checker)
			switch {
			case tc.recordChild:
				require.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, tc.currentRunID, request.ParentExecution.GetRunId())
			default:
				require.Same(t, tc.wantSameErr, err)
				require.Nil(t, resp)
				require.Equal(t, tc.recordedRunID, request.ParentExecution.GetRunId())
			}
		})
	}
}

// A closed current run selected only by workflow ID is not a reset successor to follow.
func TestRecordChildWorkflowCompletedClosedCurrentRunDoesNotFollowReset(t *testing.T) {
	const (
		parentWorkflowID = "parent"
		oldRunID         = "old-parent-run"
		currentRunID     = "current-parent-run"
	)
	recordedMissing := serviceerror.NewNotFound("workflow execution not found for workflow ID \"parent\" and run ID \"old-parent-run\"")
	ctrl := gomock.NewController(t)
	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: tests.NamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			WorkflowId: parentWorkflowID,
			RunId:      oldRunID,
		},
		ChildExecution:           &commonpb.WorkflowExecution{WorkflowId: "child-wf"},
		ChildFirstExecutionRunId: "child-first-run",
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	lease := ndc.NewMockWorkflow(ctrl)
	lease.EXPECT().GetMutableState().Return(mutableState).AnyTimes()
	lease.EXPECT().GetReleaseFn().Return(func(error) {})

	checker := api.NewMockWorkflowConsistencyChecker(ctrl)
	checker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(
		gomock.Any(), gomock.Any(), gomock.Any(),
		definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, oldRunID),
		gomock.Any(),
	).Return(nil, recordedMissing)
	checker.EXPECT().GetCurrentWorkflowRunID(
		gomock.Any(), tests.NamespaceID.String(), parentWorkflowID, locks.PriorityHigh,
	).Return(currentRunID, nil)
	checker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(
		gomock.Any(), gomock.Any(), gomock.Any(),
		definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, currentRunID),
		gomock.Any(),
	).Return(lease, nil)

	resp, err := Invoke(t.Context(), request, activeShard(t, ctrl), checker)
	require.Same(t, recordedMissing, err)
	require.Nil(t, resp)
	require.Equal(t, oldRunID, request.ParentExecution.GetRunId())
}

// A NotFound returned after the recorded parent run has been loaded is not the missing-run marker.
func TestRecordChildWorkflowCompletedUnrelatedNotFoundDoesNotFallBack(t *testing.T) {
	const (
		childWorkflowID  = "child-wf"
		childFirstRunID  = "child-first-run"
		parentWorkflowID = "parent"
		parentRunID      = "parent-run"
		initiatedEventID = int64(7)
	)
	unrelatedNotFound := serviceerror.NewNotFound("history event not found")
	for _, tc := range []struct {
		name          string
		pendingChild  *persistencespb.ChildExecutionInfo
		completionErr error
		wantErr       error
	}{
		{
			name:    "pending child is missing",
			wantErr: consts.ErrChildExecutionNotFound,
		},
		{
			name: "completion returns not found",
			pendingChild: &persistencespb.ChildExecutionInfo{
				InitiatedEventId:  initiatedEventID,
				StartedEventId:    10,
				StartedWorkflowId: childWorkflowID,
				StartedRunId:      childFirstRunID,
			},
			completionErr: unrelatedNotFound,
			wantErr:       unrelatedNotFound,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			request := &historyservice.RecordChildExecutionCompletedRequest{
				NamespaceId:       tests.NamespaceID.String(),
				ParentInitiatedId: initiatedEventID,
				ParentExecution: &commonpb.WorkflowExecution{
					WorkflowId: parentWorkflowID,
					RunId:      parentRunID,
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
				definition.NewWorkflowKey(tests.NamespaceID.String(), parentWorkflowID, parentRunID),
				gomock.Any(),
			).Return(parentCompletionLease(ctrl, initiatedEventID, tc.pendingChild, false, tc.completionErr), nil)

			resp, err := Invoke(t.Context(), request, activeShard(t, ctrl), checker)
			require.Same(t, tc.wantErr, err)
			require.Nil(t, resp)
			require.Equal(t, parentRunID, request.ParentExecution.GetRunId())
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

func parentCompletionLease(
	ctrl *gomock.Controller,
	initiatedEventID int64,
	child *persistencespb.ChildExecutionInfo,
	recordCompletion bool,
	completionErr error,
) *ndc.MockWorkflow {
	mutableState := historyi.NewMockMutableState(ctrl)
	mutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	mutableState.EXPECT().GetNextEventID().Return(int64(10))
	if child == nil {
		mutableState.EXPECT().GetChildExecutionInfo(initiatedEventID).Return(nil, false)
	} else {
		mutableState.EXPECT().GetChildExecutionInfo(initiatedEventID).Return(child, true)
	}
	if completionErr != nil {
		mutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, completionErr)
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
