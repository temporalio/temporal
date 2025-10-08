package persistence_test

import (
	"context"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	mockp "go.temporal.io/server/common/persistence/mock"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func newTestUpdateRequest(keys []tasks.Key) *p.UpdateWorkflowExecutionRequest {
	toDelete := map[tasks.Category][]tasks.Key{
		tasks.CategoryTimer: keys,
	}
	return &p.UpdateWorkflowExecutionRequest{
		ShardID: 1,
		RangeID: 1,
		Mode:    p.UpdateWorkflowModeUpdateCurrent,
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

	em := p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
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

	em := p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
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

	em := p.NewExecutionManager(
		store,
		serialization.NewSerializer(),
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
