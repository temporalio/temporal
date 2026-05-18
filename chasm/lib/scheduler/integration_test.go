package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	workflowservicemock "go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	historytasks "go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// requirePendingTypes asserts that the pending (unfired) pure tasks in the
// execution match wantTypes in scheduled-time order. Each entry is checked
// with require.Contains against the task's Go type name.
func requirePendingTypes(
	t *testing.T,
	engine *chasmtest.Engine,
	ref chasm.ComponentRef,
	wantTypes ...string,
) {
	t.Helper()
	names, err := engine.PendingPureTaskTypeNames(ref)
	require.NoError(t, err)
	require.Len(t, names, len(wantTypes), "pending pure task count mismatch")
	for i, name := range names {
		require.Contains(t, name, wantTypes[i],
			"pending task[%d]: expected type containing %q, got %q", i, wantTypes[i], name)
	}
}

// TestSingleActionSchedule tests the full lifecycle of a schedule with
// RemainingActions=1.
//
// Correct lifecycle (what should happen):
//  1. CreateSchedule → gen1 timer at first spec boundary
//  2. Gen1 fires → buffers action; InvokerProcessBufferTask fires inline:
//     decrements RemainingActions to 0, marks start ready, schedules
//     InvokerExecuteTask (side-effect). Gen1 still sees RemainingActions=1
//     when it checks idle (before the inline decrement) so it schedules gen2.
//  3. Gen2 fires → sees RemainingActions=0 → idle → schedules
//     SchedulerIdleTask at getLastEventTime()+idleTime. Because no workflow
//     has started yet, getLastEventTime()=createTime, so the idle task fires
//     at createTime+idleTime.
//  4. InvokerExecuteTask fires → starts the workflow → records
//     start.RunId and start.StartTime. After the workflow starts,
//     getLastEventTime() advances to startTime. A new idle task should be
//     scheduled at startTime+idleTime to replace the now-stale one.
//  5. (workflow runs and completes — out of scope for this test)
//  6. SchedulerIdleTask fires at startTime+idleTime → schedule closes.
//
// Actual (buggy) lifecycle:
//  Steps 1–3 match. In step 4, InvokerExecuteTask's CloseTransaction runs
//  closeTransactionCleanupInvalidTasks across every component. The
//  SchedulerIdleTask (scheduled at createTime+idleTime) is revalidated:
//  getLastEventTime() is now startTime, so idleExpiration shifts to
//  startTime+idleTime ≠ createTime+idleTime → Validate returns false → the
//  idle task is stripped from componentAttr.PureTasks with no replacement.
//  The schedule is stuck open with no pending tasks forever.
//
// This test documents the bug. The final assertion FAILS.
func TestSingleActionSchedule(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	ts := clock.NewEventTimeSource()
	createTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts.Update(createTime)

	specProcessor := newRealSpecProcessor(ctrl, logger)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))
	testEngine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	engineCtx := chasm.NewEngineContext(t.Context(), testEngine)

	executionKey := chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	}
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)

	handler := scheduler.NewTestHandler(logger)

	// Step 1: Create the schedule.
	// The inline GeneratorTask fires and schedules gen1 at the first spec boundary.
	_, err := handler.CreateSchedule(
		engineCtx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				ScheduleId: scheduleID,
				Schedule:   singleActionSchedule(),
			},
		},
	)
	require.NoError(t, err)

	requirePendingTypes(t, testEngine, rootRef, "GeneratorTask")
	allTasks, err := testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Empty(t, allTasks[historytasks.CategoryTransfer])

	// Step 2: Fire gen1.
	// Gen1 buffers the action and schedules gen2.
	// InvokerProcessBufferTask fires inline: RemainingActions→0, start ready,
	// InvokerExecuteTask queued as a side-effect transfer task.
	gen1Task := allTasks[historytasks.CategoryTimer][0]
	ts.Update(gen1Task.GetVisibilityTime())

	valid, invalid, err := testEngine.ValidateDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	require.Empty(t, invalid)
	require.Len(t, valid, 1)
	require.Contains(t, valid[0], "GeneratorTask")

	err = testEngine.ExecuteDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)

	requirePendingTypes(t, testEngine, rootRef, "GeneratorTask") // gen2 pending
	allTasks, err = testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Len(t, allTasks[historytasks.CategoryTransfer], 1, "InvokerExecuteTask queued")

	// Step 3: Fire gen2.
	// Gen2 sees RemainingActions=0 → idle → schedules SchedulerIdleTask at
	// createTime+idleTime (no workflow started yet so getLastEventTime()=createTime).
	gen2Task := allTasks[historytasks.CategoryTimer][1]
	ts.Update(gen2Task.GetVisibilityTime())

	valid, invalid, err = testEngine.ValidateDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	require.Empty(t, invalid)
	require.Len(t, valid, 1)
	require.Contains(t, valid[0], "GeneratorTask")

	err = testEngine.ExecuteDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)

	requirePendingTypes(t, testEngine, rootRef, "SchedulerIdleTask")
	allTasks, err = testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Len(t, allTasks[historytasks.CategoryTransfer], 1)

	idleTask := allTasks[historytasks.CategoryTimer][2]
	idleTime := scheduler.DefaultTweakables.IdleTime
	require.True(t,
		idleTask.GetVisibilityTime().Equal(createTime.Add(idleTime)),
		"idle task at createTime+idleTime: no workflow started so getLastEventTime()=createTime")

	// Step 4: Fire InvokerExecuteTask.
	// StartWorkflowExecution sets start.RunId and start.StartTime=time.Now().
	// CloseTransaction immediately runs closeTransactionCleanupInvalidTasks
	// across all components. The SchedulerIdleTask now fails revalidation:
	//   getLastEventTime() = startTime (>> createTime)
	//   idleExpiration     = startTime+idleTime ≠ createTime+idleTime
	// The idle task is stripped from componentAttr.PureTasks right here, with
	// no replacement scheduled.
	// → BUG: schedule is now stuck open with no pending tasks.
	mockFrontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	mockFrontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "test-run-id"}, nil)

	invoker, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (*scheduler.Invoker, error) {
			return s.Invoker.Get(ctx), nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	invokerExecuteHandler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		FrontendClient: mockFrontend,
	})

	dropped, err := chasmtest.ExecuteSideEffectTask(
		t.Context(),
		testEngine,
		invoker,
		invokerExecuteHandler,
		chasm.TaskAttributes{},
		&schedulerpb.InvokerExecuteTask{},
	)
	require.NoError(t, err)
	require.False(t, dropped)

	// After StartWorkflowExecution: the buffered start should have RunId and
	// StartTime set (from the mock response and ctx.Now()), HasCallback=true,
	// and Completed=nil (workflow is running).
	var requestID string
	_, err = chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			inv := s.Invoker.Get(ctx)
			require.Len(t, inv.BufferedStarts, 1)
			start := inv.BufferedStarts[0]
			require.Equal(t, "test-run-id", start.GetRunId())
			require.True(t, start.GetStartTime().AsTime().Equal(gen2Task.GetVisibilityTime()),
				"StartTime should equal ctx.Now() at task execution time")
			require.True(t, start.GetHasCallback())
			require.Nil(t, start.GetCompleted(), "workflow has not yet completed")
			requestID = start.GetRequestId()
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, requestID)

	// Step 5: Simulate the workflow completing via the Nexus completion callback.
	// In production the workflow sends a completion to the Nexus endpoint which
	// routes to HandleNexusCompletion on the Scheduler.
	_, _, err = chasm.UpdateComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			return nil, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: requestID,
				CloseTime: timestamppb.New(ts.Now()),
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: &commonpb.Payload{},
				},
			})
		},
		struct{}{},
	)
	require.NoError(t, err)

	// After the completion callback: the buffered start should have Completed set
	// with a successful status. RunId and StartTime remain.
	_, err = chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			inv := s.Invoker.Get(ctx)
			require.Len(t, inv.BufferedStarts, 1)
			start := inv.BufferedStarts[0]
			require.Equal(t, "test-run-id", start.GetRunId())
			require.NotNil(t, start.GetCompleted(), "completion should be recorded")
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, start.GetCompleted().GetStatus())
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	// BUG: closeTransactionCleanupInvalidTasks ran during InvokerExecuteTask's
	// CloseTransaction and stripped the idle task because idleExpiration shifted
	// from createTime+idleTime to startTime+idleTime. No replacement was scheduled.
	// After the fix, InvokerExecuteTask should reschedule the idle task at the
	// new expiration (startTime+idleTime) so the schedule can still close.
	// startTime = ctx.Now() = gen2Task.GetVisibilityTime(), so the correct
	// idle expiration is gen2Task.GetVisibilityTime()+idleTime.
	requirePendingTypes(t, testEngine, rootRef) // empty — should be ["SchedulerIdleTask"] at gen2Time+idleTime

	// Advance time to gen2Time+idleTime: the point at which the replacement
	// idle task should fire after the fix.
	expectedIdleExpiration := gen2Task.GetVisibilityTime().Add(idleTime)
	ts.Update(expectedIdleExpiration)

	valid, invalid, err = testEngine.ValidateDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	require.Empty(t, invalid)
	// After the fix: valid should contain "SchedulerIdleTask".
	require.Len(t, valid, 1, "idle task at gen2Time+idleTime should be due and valid")
	require.Contains(t, valid[0], "SchedulerIdleTask")

	err = testEngine.ExecuteDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)

	sched, err := chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	// After the fix: the idle task fires at gen2Time+idleTime and sets Closed=true.
	require.True(t, sched.Closed, "schedule should be closed after idle task fires at gen2Time+idleTime")
}
