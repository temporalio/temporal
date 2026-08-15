package persistence_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func newTestUpdateRequest(keys []tasks.Key) *p.UpdateWorkflowExecutionRequest {
	toDelete := map[tasks.Category][]tasks.Key{
		tasks.CategoryTimer: keys,
	}
	return &p.UpdateWorkflowExecutionRequest{
		ShardID:     1,
		RangeID:     1,
		Mode:        p.UpdateWorkflowModeUpdateCurrent,
		ArchetypeID: chasm.WorkflowArchetypeID,
		UpdateWorkflowMutation: p.WorkflowMutation{
			ExecutionInfo:         &persistencespb.WorkflowExecutionInfo{NamespaceId: "ns", WorkflowId: "wid", ExecutionStats: &persistencespb.ExecutionStats{}},
			ExecutionState:        &persistencespb.WorkflowExecutionState{RunId: "rid", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
			NextEventID:           2,
			BestEffortDeleteTasks: toDelete,
		},
	}
}

func TestExecutionManager_DeletesTasksWhenEnabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	// Expect the main update call
	store.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	// Expect CompleteHistoryTask for each key
	// Use gomock.Any for ctx and request pointer type
	expectedKeys := []tasks.Key{
		tasks.NewKey(time.Now().UTC(), 123),
		tasks.NewKey(time.Now().UTC().Add(time.Second), 456),
	}
	// Allow any order
	store.EXPECT().CompleteHistoryTask(gomock.Any(), gomock.Any()).Times(len(expectedKeys)).DoAndReturn(
		func(_ context.Context, req *p.CompleteHistoryTaskRequest) error {
			if req.ShardID != 1 || req.TaskCategory != tasks.CategoryTimer {
				t.Fatalf("unexpected CompleteHistoryTask request: %#v", req)
			}
			return nil
		},
	)

	serializer := serialization.NewSerializer()
	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(true),
	)

	_, err := em.UpdateWorkflowExecution(context.Background(), newTestUpdateRequest(expectedKeys))
	if err != nil {
		t.Fatalf("UpdateWorkflowExecution returned error: %v", err)
	}
}

func TestExecutionManager_DoesNotDeleteTasksWhenDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	store.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	// No CompleteHistoryTask expected

	serializer := serialization.NewSerializer()
	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	keys := []tasks.Key{tasks.NewKey(time.Now().UTC(), 789)}
	_, err := em.UpdateWorkflowExecution(context.Background(), newTestUpdateRequest(keys))
	if err != nil {
		t.Fatalf("UpdateWorkflowExecution returned error: %v", err)
	}
}

func TestExecutionManager_DeleteTasksError_DoesNotFailUpdate(t *testing.T) {
	// Test that errors in CompleteHistoryTask don't cause the UpdateWorkflowExecution to fail.
	// Task deletion is best-effort and should only be logged.
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	// Expect the main update call to succeed
	store.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	// Expect CompleteHistoryTask to fail
	expectedKeys := []tasks.Key{
		tasks.NewKey(time.Now().UTC(), 123),
	}
	store.EXPECT().CompleteHistoryTask(gomock.Any(), gomock.Any()).Times(len(expectedKeys)).Return(
		serviceerror.NewInternal("simulated deletion error"),
	)

	serializer := serialization.NewSerializer()
	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(true),
	)

	// UpdateWorkflowExecution should succeed even though CompleteHistoryTask failed
	_, err := em.UpdateWorkflowExecution(context.Background(), newTestUpdateRequest(expectedKeys))
	if err != nil {
		t.Fatalf("UpdateWorkflowExecution should succeed even if task deletion fails, got error: %v", err)
	}
}

func TestExecutionManager_TrimHistoryBranchSkipped_NonWorkflow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	store.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&p.ConditionFailedError{Msg: "conflict"})
	// trimHistoryNode logic skipped for non-workflow archetype and should be no call to GetWorkflowExecution.
	store.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Times(0)

	serializer := serialization.NewSerializer()
	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		// TrimHistoryBranch logic is best effort and only emits error log if failed.
		// Capture error logs to verify no unexpected errors.
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	req := newTestUpdateRequest(nil)
	req.ArchetypeID = chasm.WorkflowArchetypeID + 1 // Non-workflow archetype
	_, err := em.UpdateWorkflowExecution(context.Background(), req)
	if err == nil {
		t.Fatal("expected UpdateWorkflowExecution to return error")
	}
	if _, ok := err.(*p.ConditionFailedError); !ok {
		t.Fatalf("expected ConditionFailedError, got %T", err)
	}
}

func TestExecutionManager_TrimHistoryBranchSkipped_EmptyBranchToken(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	store.EXPECT().UpdateWorkflowExecution(gomock.Any(), gomock.Any()).Return(&p.ConditionFailedError{Msg: "conflict"})
	store.EXPECT().GetHistoryBranchUtil().Times(0)

	serializer := serialization.NewSerializer()
	versionHistories := versionhistory.NewVersionHistories(&historyspb.VersionHistory{}) // Empty branch token
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:      "ns",
		WorkflowId:       "wid",
		ExecutionStats:   &persistencespb.ExecutionStats{},
		VersionHistories: versionHistories,
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:  "rid",
		State:  enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}
	executionInfoBlob, err := serializer.WorkflowExecutionInfoToBlob(executionInfo)
	if err != nil {
		t.Fatalf("WorkflowExecutionInfoToBlob error: %v", err)
	}
	executionStateBlob, err := serializer.WorkflowExecutionStateToBlob(executionState)
	if err != nil {
		t.Fatalf("WorkflowExecutionStateToBlob error: %v", err)
	}

	store.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(
		&p.InternalGetWorkflowExecutionResponse{
			State: &p.InternalWorkflowMutableState{
				ExecutionInfo:  executionInfoBlob,
				ExecutionState: executionStateBlob,
			},
		},
		nil,
	)

	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		// TrimHistoryBranch logic is best effort and only emits error log if failed.
		// Capture error logs to verify no unexpected errors.
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	_, err = em.UpdateWorkflowExecution(context.Background(), newTestUpdateRequest(nil))
	if err == nil {
		t.Fatal("expected UpdateWorkflowExecution to return error")
	}
	if _, ok := err.(*p.ConditionFailedError); !ok {
		t.Fatalf("expected ConditionFailedError, got %T", err)
	}
}

func TestExecutionManager_ConflictResolveExpectedCurrentRunID(t *testing.T) {
	tests := []struct {
		name                 string
		expectedCurrentRunID string
		currentMutationRunID string
		wantRunID            string
	}{
		{
			name:                 "explicit without current mutation",
			expectedCurrentRunID: "current-run-id",
			wantRunID:            "current-run-id",
		},
		{
			name:                 "explicit matching current mutation",
			expectedCurrentRunID: "current-run-id",
			currentMutationRunID: "current-run-id",
			wantRunID:            "current-run-id",
		},
		{
			name:                 "legacy current mutation fallback",
			currentMutationRunID: "current-run-id",
			wantRunID:            "current-run-id",
		},
		{
			name:      "legacy reset workflow fallback",
			wantRunID: "reset-run-id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := mockp.NewMockExecutionStore(ctrl)
			store.EXPECT().GetName().AnyTimes().Return("mock-store")
			store.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
				func(_ context.Context, request *p.InternalConflictResolveWorkflowExecutionRequest) error {
					require.Equal(t, tt.wantRunID, request.ExpectedCurrentRunID)
					return nil
				},
			)

			em := newTestExecutionManager(store)
			request := newTestConflictResolveRequest(p.ConflictResolveWorkflowModeUpdateCurrent)
			request.ExpectedCurrentRunID = tt.expectedCurrentRunID
			if tt.currentMutationRunID != "" {
				mutation := newTestConflictResolveMutation(tt.currentMutationRunID)
				request.CurrentWorkflowMutation = &mutation
			}

			_, err := em.ConflictResolveWorkflowExecution(context.Background(), request)
			require.NoError(t, err)
		})
	}
}

func TestExecutionManager_ConflictResolveRejectsInvalidExpectedCurrentRunID(t *testing.T) {
	tests := []struct {
		name    string
		request *p.ConflictResolveWorkflowExecutionRequest
	}{
		{
			name: "does not match current mutation",
			request: func() *p.ConflictResolveWorkflowExecutionRequest {
				request := newTestConflictResolveRequest(p.ConflictResolveWorkflowModeUpdateCurrent)
				request.ExpectedCurrentRunID = "other-run-id"
				mutation := newTestConflictResolveMutation("current-run-id")
				request.CurrentWorkflowMutation = &mutation
				return request
			}(),
		},
		{
			name: "set while bypassing current",
			request: func() *p.ConflictResolveWorkflowExecutionRequest {
				request := newTestConflictResolveRequest(p.ConflictResolveWorkflowModeBypassCurrent)
				request.ExpectedCurrentRunID = "current-run-id"
				return request
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			store := mockp.NewMockExecutionStore(ctrl)
			store.EXPECT().GetName().AnyTimes().Return("mock-store")

			em := newTestExecutionManager(store)
			_, err := em.ConflictResolveWorkflowExecution(context.Background(), tt.request)
			var internalErr *serviceerror.Internal
			require.ErrorAs(t, err, &internalErr)
		})
	}
}

func TestExecutionManager_ConflictResolveBypassCurrentForwardsEmptyExpectedRunID(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")
	store.EXPECT().ConflictResolveWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, request *p.InternalConflictResolveWorkflowExecutionRequest) error {
			require.Empty(t, request.ExpectedCurrentRunID)
			return nil
		},
	)

	em := newTestExecutionManager(store)
	_, err := em.ConflictResolveWorkflowExecution(
		context.Background(),
		newTestConflictResolveRequest(p.ConflictResolveWorkflowModeBypassCurrent),
	)
	require.NoError(t, err)
}

func newTestExecutionManager(store p.ExecutionStore) p.ExecutionManager {
	return p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
		nil,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)
}

func newTestConflictResolveRequest(mode p.ConflictResolveWorkflowMode) *p.ConflictResolveWorkflowExecutionRequest {
	return &p.ConflictResolveWorkflowExecutionRequest{
		ShardID:               1,
		RangeID:               1,
		Mode:                  mode,
		ArchetypeID:           chasm.WorkflowArchetypeID,
		ResetWorkflowSnapshot: newTestConflictResolveSnapshot("reset-run-id"),
	}
}

func newTestConflictResolveSnapshot(runID string) p.WorkflowSnapshot {
	return p.WorkflowSnapshot{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:    "namespace-id",
			WorkflowId:     "workflow-id",
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  runID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}
}

func newTestConflictResolveMutation(runID string) p.WorkflowMutation {
	return p.WorkflowMutation{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			NamespaceId:    "namespace-id",
			WorkflowId:     "workflow-id",
			ExecutionStats: &persistencespb.ExecutionStats{},
		},
		ExecutionState: &persistencespb.WorkflowExecutionState{
			RunId:  runID,
			State:  enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		},
	}
}
