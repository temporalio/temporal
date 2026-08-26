package persistence_test

import (
	"context"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
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

func TestExecutionManager_GetWorkflowExecutionLeavesTimerInfosEncoded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")

	serializer := serialization.NewSerializer()
	timerInfoBlob, err := serializer.TimerInfoToBlob(&persistencespb.TimerInfo{TimerId: "t1", StartedEventId: 5})
	if err != nil {
		t.Fatalf("TimerInfoToBlob error: %v", err)
	}
	executionInfo := &persistencespb.WorkflowExecutionInfo{
		NamespaceId:    "namespace",
		WorkflowId:     "workflow",
		UserTimerCount: 1,
	}
	executionState := &persistencespb.WorkflowExecutionState{
		RunId:  "run",
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
				TimerInfos:     map[string]*commonpb.DataBlob{"t1": timerInfoBlob},
			},
			DBRecordVersion: 3,
		},
		nil,
	)

	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	resp, err := em.GetWorkflowExecution(context.Background(), &p.GetWorkflowExecutionRequest{
		ShardID:     1,
		NamespaceID: "namespace",
		WorkflowID:  "workflow",
		RunID:       "run",
		ArchetypeID: chasm.WorkflowArchetypeID,
	})
	if err != nil {
		t.Fatalf("GetWorkflowExecution error: %v", err)
	}
	if len(resp.State.TimerInfos) != 0 {
		t.Fatalf("expected state timer infos to be left empty, got %v", resp.State.TimerInfos)
	}
	if len(resp.TimerInfoBlobs) != 1 || resp.TimerInfoBlobs["t1"] != timerInfoBlob {
		t.Fatalf("expected timer info blob to be passed through verbatim, got %v", resp.TimerInfoBlobs)
	}
	if resp.MutableStateStats.TimerInfoCount != 1 || resp.MutableStateStats.TotalUserTimerCount != 1 {
		t.Fatalf("unexpected timer stats: %+v", resp.MutableStateStats)
	}
	if resp.DBRecordVersion != 3 {
		t.Fatalf("unexpected DB record version: %v", resp.DBRecordVersion)
	}
}

func TestExecutionManager_SetWorkflowExecutionPersistsEncodedTimerInfosVerbatim(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")

	serializer := serialization.NewSerializer()
	encodedTimerBlob, err := serializer.TimerInfoToBlob(&persistencespb.TimerInfo{TimerId: "t-encoded", StartedEventId: 5})
	if err != nil {
		t.Fatalf("TimerInfoToBlob error: %v", err)
	}
	decodedTimer := &persistencespb.TimerInfo{TimerId: "t-decoded", StartedEventId: 6}

	var captured *p.InternalSetWorkflowExecutionRequest
	store.EXPECT().SetWorkflowExecution(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, req *p.InternalSetWorkflowExecutionRequest) error {
			captured = req
			return nil
		},
	)

	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	_, err = em.SetWorkflowExecution(context.Background(), &p.SetWorkflowExecutionRequest{
		ShardID:     1,
		RangeID:     1,
		ArchetypeID: chasm.WorkflowArchetypeID,
		SetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: "namespace", WorkflowId: "workflow"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"},
			TimerInfos:     map[string]*persistencespb.TimerInfo{"t-decoded": decodedTimer},
			TimerInfoBlobs: map[string]*commonpb.DataBlob{"t-encoded": encodedTimerBlob},
		},
	})
	if err != nil {
		t.Fatalf("SetWorkflowExecution error: %v", err)
	}

	persistedTimerInfos := captured.SetWorkflowSnapshot.TimerInfos
	if len(persistedTimerInfos) != 2 {
		t.Fatalf("expected both timer entries to be persisted, got %v", persistedTimerInfos)
	}
	if persistedTimerInfos["t-encoded"] != encodedTimerBlob {
		t.Fatal("expected encoded timer entry to be persisted verbatim")
	}
	roundTrippedTimer, err := serializer.TimerInfoFromBlob(persistedTimerInfos["t-decoded"])
	if err != nil {
		t.Fatalf("TimerInfoFromBlob error: %v", err)
	}
	if roundTrippedTimer.String() != decodedTimer.String() {
		t.Fatalf("expected decoded timer to survive a round trip, got %v", roundTrippedTimer)
	}
}

func TestExecutionManager_SetWorkflowExecutionRejectsDuplicateTimerInfos(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store := mockp.NewMockExecutionStore(ctrl)
	store.EXPECT().GetName().AnyTimes().Return("mock-store")

	serializer := serialization.NewSerializer()
	timerBlob, err := serializer.TimerInfoToBlob(&persistencespb.TimerInfo{TimerId: "dup"})
	if err != nil {
		t.Fatalf("TimerInfoToBlob error: %v", err)
	}

	// no store expectations, serialization must fail before reaching the store
	em := p.NewExecutionManager(
		store,
		serializer,
		nil,
		testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError),
		dynamicconfig.GetIntPropertyFn(1024*1024),
		dynamicconfig.GetBoolPropertyFn(false),
	)

	_, err = em.SetWorkflowExecution(context.Background(), &p.SetWorkflowExecutionRequest{
		ShardID:     1,
		RangeID:     1,
		ArchetypeID: chasm.WorkflowArchetypeID,
		SetWorkflowSnapshot: p.WorkflowSnapshot{
			ExecutionInfo:  &persistencespb.WorkflowExecutionInfo{NamespaceId: "namespace", WorkflowId: "workflow"},
			ExecutionState: &persistencespb.WorkflowExecutionState{RunId: "run"},
			TimerInfos:     map[string]*persistencespb.TimerInfo{"dup": {TimerId: "dup"}},
			TimerInfoBlobs: map[string]*commonpb.DataBlob{"dup": timerBlob},
		},
	})
	if err == nil {
		t.Fatal("expected SetWorkflowExecution to reject duplicate user timer entries")
	}
}
