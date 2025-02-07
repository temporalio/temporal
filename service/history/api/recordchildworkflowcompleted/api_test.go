// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package recordchildworkflowcompleted

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
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
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(&namespace.Namespace{}, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := shard.NewMockContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := workflow.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo)

	newParentMutableState := workflow.NewMockMutableState(ctrl)
	newParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	newParentMutableState.EXPECT().GetNextEventID().Return(int64(10))
	newParentMutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(anyArg, anyArg, anyArg).Return(nil, nil)
	childExecutionInfo := &persistencespb.ChildExecutionInfo{
		StartedEventId:    int64(10), // indicate that the started event is already recorded.
		StartedWorkflowId: childWFID,
	}
	newParentMutableState.EXPECT().GetChildExecutionInfo(anyArg).Return(childExecutionInfo, true)
	newParentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	newParentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(anyArg, anyArg).Return(nil, nil)
	newParentMutableState.EXPECT().GetChildrenInitializedPostResetPoint().Return(nil)

	mockWFContext := workflow.NewMockContext(ctrl)
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
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(&namespace.Namespace{}, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := shard.NewMockContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := workflow.NewMockMutableState(ctrl)
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

// Tests that the parent successfully records a completion event from a child that was restarted due to parent reset.
// It mainly asserts that the ChildrenInitializedPostResetPoint in mtrent utable state is properly updated.
func Test_Recordchildworkflowcompleted_FromRestartedChild(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFType := uuid.NewString()
	childWFID := uuid.NewString()
	resetChildID := childWFType + ":" + childWFID
	paretntWFID := uuid.NewString()
	newParentRunID := uuid.NewString()
	newParentWFKey := definition.NewWorkflowKey(testNamespaceID.String(), paretntWFID, newParentRunID)

	// The request will be sent to the old parent.
	request := &historyservice.RecordChildExecutionCompletedRequest{
		NamespaceId: testNamespaceID.String(),
		ParentExecution: &commonpb.WorkflowExecution{
			RunId:      newParentRunID,
			WorkflowId: paretntWFID,
		},
		ChildExecution: &commonpb.WorkflowExecution{WorkflowId: childWFID},
		CompletionEvent: &historypb.HistoryEvent{
			EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		},
	}
	mockRegistery := namespace.NewMockRegistry(ctrl)
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(&namespace.Namespace{}, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := shard.NewMockContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	childrenInitializedPostResetPoint := map[string]*persistencespb.ResetChildInfo{
		resetChildID:   {},
		"AnotherChild": {},
	}
	newParentMutableState := workflow.NewMockMutableState(ctrl)
	newParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	newParentMutableState.EXPECT().GetNextEventID().Return(int64(10))
	newParentMutableState.EXPECT().AddChildWorkflowExecutionCompletedEvent(anyArg, anyArg, anyArg).Return(nil, nil)
	childExecutionInfo := &persistencespb.ChildExecutionInfo{
		StartedEventId:    int64(10), // indicate that the started event is already recorded.
		StartedWorkflowId: childWFID,
	}
	newParentMutableState.EXPECT().GetChildExecutionInfo(anyArg).Return(childExecutionInfo, true)
	newParentMutableState.EXPECT().HasPendingWorkflowTask().Return(false)
	newParentMutableState.EXPECT().AddWorkflowTaskScheduledEvent(anyArg, anyArg).Return(nil, nil)
	newParentMutableState.EXPECT().GetChildrenInitializedPostResetPoint().Return(childrenInitializedPostResetPoint)
	newParentMutableState.EXPECT().SetChildrenInitializedPostResetPoint(childrenInitializedPostResetPoint).
		Do(func(resetChildInfoMap map[string]*persistencespb.ResetChildInfo) {
			require.NotContains(t, resetChildInfoMap, resetChildID) // assert that the entry for the child is removed.
			require.Contains(t, resetChildInfoMap, "AnotherChild")  // assert that the entry for the unrelated child is not affected.
		})
	newParentMutableState.EXPECT().GetChildExecutionInitiatedEvent(anyArg, anyArg).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: childWFType},
				WorkflowId:   childWFID,
			},
		},
	}, nil)

	mockWFContext := workflow.NewMockContext(ctrl)
	mockWFContext.EXPECT().UpdateWorkflowExecutionAsActive(anyArg, anyArg).Return(nil)

	newParentWFLease := ndc.NewMockWorkflow(ctrl)
	newParentWFLease.EXPECT().GetMutableState().Return(newParentMutableState).AnyTimes() // new parent's mutable state would be accessed many times.
	newParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease.EXPECT().GetContext().Return(mockWFContext)

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, newParentWFKey, anyArg).Return(newParentWFLease, nil)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.NoError(t, err)
	require.NotNil(t, resp)
}

// This test asserts that we don't accept results from an old child when the parent is reset and the child is restarted.
// This is to prevent the scenario where the child races to complete before the parent can successfully "restart" the child.
func Test_Recordchildworkflowcompleted_WithForwardsFromStaleChild(t *testing.T) {
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	anyArg := gomock.Any()

	testNamespaceID := tests.NamespaceID
	childWFType := uuid.NewString()
	childWFID := uuid.NewString()
	resetChildID := childWFType + ":" + childWFID
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
	mockRegistery.EXPECT().GetNamespaceByID(testNamespaceID).Return(&namespace.Namespace{}, nil)
	mockClusterMetadata := cluster.NewMockMetadata(ctrl)
	mockClusterMetadata.EXPECT().GetCurrentClusterName().Return("")
	shardContext := shard.NewMockContext(ctrl)
	shardContext.EXPECT().GetNamespaceRegistry().Return(mockRegistery)
	shardContext.EXPECT().GetClusterMetadata().Return(mockClusterMetadata)

	oldParentMutableState := workflow.NewMockMutableState(ctrl)
	oldParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(false)
	oldParentMutableState.EXPECT().GetExecutionInfo().Return(oldParentExecutionInfo)

	newParentMutableState := workflow.NewMockMutableState(ctrl)
	newParentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	newParentMutableState.EXPECT().GetNextEventID().Return(int64(10))
	childExecutionInfo := &persistencespb.ChildExecutionInfo{
		StartedEventId:    int64(10), // indicate that the started event is already recorded.
		StartedWorkflowId: childWFID,
	}
	newParentMutableState.EXPECT().GetChildExecutionInfo(anyArg).Return(childExecutionInfo, true)
	newParentMutableState.EXPECT().GetChildrenInitializedPostResetPoint().Return(map[string]*persistencespb.ResetChildInfo{
		resetChildID: {},
	})
	newParentMutableState.EXPECT().GetChildExecutionInitiatedEvent(anyArg, anyArg).Return(&historypb.HistoryEvent{
		EventType: enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED,
		Attributes: &historypb.HistoryEvent_StartChildWorkflowExecutionInitiatedEventAttributes{
			StartChildWorkflowExecutionInitiatedEventAttributes: &historypb.StartChildWorkflowExecutionInitiatedEventAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: childWFType},
				WorkflowId:   childWFID,
			},
		},
	}, nil)

	oldParentWFLease := ndc.NewMockWorkflow(ctrl)
	oldParentWFLease.EXPECT().GetMutableState().Return(oldParentMutableState) // old parent's mutable state is accessed just once.
	oldParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})
	newParentWFLease := ndc.NewMockWorkflow(ctrl)
	newParentWFLease.EXPECT().GetMutableState().Return(newParentMutableState).AnyTimes() // new parent's mutable state would be accessed many times.
	newParentWFLease.EXPECT().GetReleaseFn().Return(func(_ error) {})

	consistencyChecker := api.NewMockWorkflowConsistencyChecker(ctrl)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, oldParentWFKey, anyArg).Return(oldParentWFLease, nil)
	consistencyChecker.EXPECT().GetWorkflowLeaseWithConsistencyCheck(anyArg, anyArg, anyArg, newParentWFKey, anyArg).Return(newParentWFLease, nil)

	resp, err := Invoke(ctx, request, shardContext, consistencyChecker)
	require.ErrorIs(t, err, consts.ErrChildExecutionNotFound)
	require.Nil(t, resp)
	require.Equal(t, newParentRunID, request.ParentExecution.RunId) // the request should be modified to point to the new parent.
}
