package scheduler_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
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

// calendarSchedule returns a schedule that fires exactly once at each of the
// provided times. Each time is expressed as a StructuredCalendarSpec pinned to
// the exact year/month/day/hour/minute/second, so the generator sees
// nextWakeupTime=zero after the last firing and goes idle without needing
// LimitedActions.
//
// Note: DayOfWeek must cover all days — makeBitMatcher(nil) produces bits=0
// which silently blocks every time.
func calendarSchedule(times ...time.Time) *schedulepb.Schedule {
	var entries []*schedulepb.StructuredCalendarSpec
	for _, ts := range times {
		t := ts.UTC()
		entries = append(entries, &schedulepb.StructuredCalendarSpec{
			Second:     []*schedulepb.Range{{Start: int32(t.Second()), End: int32(t.Second()), Step: 1}},
			Minute:     []*schedulepb.Range{{Start: int32(t.Minute()), End: int32(t.Minute()), Step: 1}},
			Hour:       []*schedulepb.Range{{Start: int32(t.Hour()), End: int32(t.Hour()), Step: 1}},
			DayOfMonth: []*schedulepb.Range{{Start: int32(t.Day()), End: int32(t.Day()), Step: 1}},
			Month:      []*schedulepb.Range{{Start: int32(t.Month()), End: int32(t.Month()), Step: 1}},
			Year:       []*schedulepb.Range{{Start: int32(t.Year()), End: int32(t.Year()), Step: 1}},
			DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6, Step: 1}},
		})
	}
	return &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{StructuredCalendar: entries},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "scheduled-wf",
					WorkflowType: &commonpb.WorkflowType{Name: "scheduled-wf-type"},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{},
		State:    &schedulepb.ScheduleState{},
	}
}

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

// TestSingleActionSchedule verifies that a single-date calendar schedule closes
// after its one workflow completes. Each step is one transaction.
func TestSingleActionSchedule(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	mockFrontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	mockFrontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "test-run-id"}, nil)
	ts := clock.NewEventTimeSource()
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC) // schedule created
	t1 := t0.Add(time.Hour)                           // calendar spec fires; workflow starts
	t2 := t1.Add(time.Minute)                         // workflow completes
	idleTime := scheduler.DefaultTweakables.IdleTime
	ts.Update(t0)

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

	// Txn 1 [t=t0]: StartExecution (CreateSchedule).
	// The inline GeneratorTask fires and sees no actions in [t0, t0]
	// (t1 is in the future), so it schedules a generator timer at t1.
	_, err := handler.CreateSchedule(
		engineCtx,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				ScheduleId: scheduleID,
				Schedule:   calendarSchedule(t1),
			},
		},
	)
	require.NoError(t, err)

	allTasks, err := testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Empty(t, allTasks[historytasks.CategoryTransfer])
	require.Len(t, allTasks[historytasks.CategoryTimer], 1)

	genTask := allTasks[historytasks.CategoryTimer][0]
	require.True(t, genTask.GetVisibilityTime().Equal(t1),
		"generator task should be scheduled at t1")
	genTaskType, err := testEngine.PureTaskTypeName(rootRef, genTask.GetVisibilityTime())
	require.NoError(t, err)
	require.Contains(t, genTaskType, "GeneratorTask")

	// -- time: t0 → t1 --
	// Txn 2 [t=t1]: executeChasmPureTimers (GeneratorTask).
	// ProcessTimeRange([t0, t1]) finds the single calendar action at t1.
	// The generator sees nextWakeupTime=zero → idle → schedules SchedulerIdleTask
	// at t0+idleTime (getLastEventTime()=t0, no workflow started yet), then
	// returns. CloseTransaction fires InvokerProcessBufferTask inline via
	// executeImmediatePureTasks: marks start ready, schedules InvokerExecuteTask.
	ts.Update(t1)

	valid, invalid, err := testEngine.ValidateDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	require.Empty(t, invalid)
	require.Len(t, valid, 1)
	require.Contains(t, valid[0], "GeneratorTask")

	err = testEngine.ExecuteChasmPureTimers(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)

	allTasks, err = testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Len(t, allTasks[historytasks.CategoryTransfer], 1, "InvokerExecuteTask queued")
	require.Len(t, allTasks[historytasks.CategoryTimer], 2)

	idleTask := allTasks[historytasks.CategoryTimer][1]
	require.True(t,
		idleTask.GetVisibilityTime().Equal(t0.Add(idleTime)),
		"idle task at t0+idleTime: getLastEventTime()=t0 because no workflow started yet")
	idleTaskType, err := testEngine.PureTaskTypeName(rootRef, idleTask.GetVisibilityTime())
	require.NoError(t, err)
	require.Contains(t, idleTaskType, "SchedulerIdleTask")

	// Txn 3 [t=t1]: executeChasmSideEffectTask (InvokerExecuteTask).
	// StartWorkflowExecution → start.StartTime=t1 (ctx.Now()).
	// CloseTransaction runs closeTransactionCleanupInvalidTasks: the SchedulerIdleTask
	// (at t0+idleTime) revalidates with getLastEventTime()=t1 → idleExpiration=t3
	// ≠ t0+idleTime → Validate returns false → idle task stripped, no replacement.
	// → BUG: schedule stuck open.

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

	// Txn 3 closed. closeTransactionCleanupInvalidTasks ran on all components
	// because invoker state changed (isActiveStateDirty=true). The SchedulerIdleTask
	// revalidated with getLastEventTime()=t1 → idleExpiration=t3 ≠ t0+idleTime
	// → dropped silently with no replacement (BUG).
	//
	// ValidateAllPendingTasks mirrors what cleanup does: validate every pending
	// task regardless of scheduled time. After Txn 3 nothing remains because
	// cleanup already ran — the empty result confirms the task was dropped.
	cleanupValid, cleanupInvalid, err := testEngine.ValidateAllPendingTasks(t.Context(), rootRef)
	require.NoError(t, err)
	require.Empty(t, cleanupValid)
	require.Empty(t, cleanupInvalid, "idle task already dropped by cleanup during Txn 3")

	// After StartWorkflowExecution: start.RunId and start.StartTime are set
	// (StartTime = time.Now() at RPC completion), HasCallback=true, Completed=nil.
	var requestID string
	_, err = chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			inv := s.Invoker.Get(ctx)
			require.Len(t, inv.BufferedStarts, 1)
			start := inv.BufferedStarts[0]
			require.Equal(t, "test-run-id", start.GetRunId())
			require.False(t, start.GetStartTime().AsTime().IsZero(), "start.StartTime should be set")
			require.True(t, start.GetHasCallback())
			require.Nil(t, start.GetCompleted(), "workflow has not yet completed")
			requestID = start.GetRequestId()
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	require.NotEmpty(t, requestID)

	// -- time: t1 → t2 --
	// Txn 4 [t=t2]: CompleteNexusOperationChasm (workflow completion callback).
	// HandleNexusCompletion records start.Completed.
	ts.Update(t2)
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

	// After the completion callback: Completed is set, workflow status is recorded.
	// generator.Generate() fired inline during CloseTransaction, re-evaluating
	// idle state with getLastEventTime()=t1 → schedules SchedulerIdleTask at t3.
	_, err = chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			inv := s.Invoker.Get(ctx)
			require.Len(t, inv.BufferedStarts, 1)
			start := inv.BufferedStarts[0]
			require.Equal(t, "test-run-id", start.GetRunId())
			require.NotNil(t, start.GetCompleted())
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, start.GetCompleted().GetStatus())
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
	requirePendingTypes(t, testEngine, rootRef, "SchedulerIdleTask")

	// The idle task is at start.StartTime+idleTime. start.StartTime = time.Now()
	// (wall clock at RPC completion), so we read the actual visibility time from
	// the engine rather than relying on the controlled clock.
	allTasksAfterCompletion, err := testEngine.Tasks(rootRef)
	require.NoError(t, err)
	idleTaskVisibility := allTasksAfterCompletion[historytasks.CategoryTimer][len(allTasksAfterCompletion[historytasks.CategoryTimer])-1].GetVisibilityTime()

	// -- time: t2 → idleTaskVisibility --
	// Txn 5: executeChasmPureTimers (SchedulerIdleTask).
	ts.Update(idleTaskVisibility)

	valid, invalid, err = testEngine.ValidateDuePureTasks(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	require.Empty(t, invalid)
	// After the fix: valid should contain "SchedulerIdleTask" at t3.
	require.Len(t, valid, 1, "idle task at t3 should be due and valid")
	require.Contains(t, valid[0], "SchedulerIdleTask")

	err = testEngine.ExecuteChasmPureTimers(t.Context(), rootRef, ts.Now())
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
	require.True(t, sched.Closed, "schedule should be closed after idle task fires")

	completed, err := testEngine.IsExecutionCompleted(rootRef)
	require.NoError(t, err)
	require.True(t, completed, "execution should be in COMPLETED state")
}

// TestTwoActionSchedule verifies that a two-date calendar schedule closes
// after both workflows complete. Each step is one transaction.
func TestTwoActionSchedule(t *testing.T) {
	ctrl := gomock.NewController(t)
	logger := log.NewTestLogger()
	mockFrontend := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	mockFrontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-1"}, nil)
	mockFrontend.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-2"}, nil)

	ts := clock.NewEventTimeSource()
	t0 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(defaultInterval) // first spec boundary
	t2 := t1.Add(time.Second)     // workflow 1 completes (before second boundary)
	t3 := t1.Add(defaultInterval) // second spec boundary
	t4 := t3.Add(time.Second)     // workflow 2 completes
	ts.Update(t0)

	specProcessor := newRealSpecProcessor(ctrl, logger)
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(newTestLibrary(logger, specProcessor)))
	testEngine := chasmtest.NewEngine(t, registry, chasmtest.WithTimeSource(ts))
	engineCtx := chasm.NewEngineContext(t.Context(), testEngine)

	schedule := calendarSchedule(t1, t3)

	executionKey := chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID}
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](executionKey)

	invokerExecuteHandler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		SpecProcessor:  specProcessor,
		FrontendClient: mockFrontend,
	})

	// Txn 1 [t=t0]: create.
	_, err := scheduler.NewTestHandler(logger).CreateSchedule(engineCtx, &schedulerpb.CreateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.CreateScheduleRequest{
			ScheduleId: scheduleID,
			Schedule:   schedule,
		},
	})
	require.NoError(t, err)
	requirePendingTypes(t, testEngine, rootRef, "GeneratorTask")

	// -- time: t0 → t1 --
	// Txn 2 [t=t1]: executeChasmPureTimers (gen1).
	// Buffers action1; InvokerProcessBufferTask fires inline: remaining 2→1,
	// schedules InvokerExecuteTask. Generator sees remaining=2 at idle check
	// → not idle → schedules gen2 at t3.
	ts.Update(t1)
	err = testEngine.ExecuteChasmPureTimers(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	requirePendingTypes(t, testEngine, rootRef, "GeneratorTask") // gen2 at t3
	allTasks, err := testEngine.Tasks(rootRef)
	require.NoError(t, err)
	require.Len(t, allTasks[historytasks.CategoryTransfer], 1)
	gen2Task := allTasks[historytasks.CategoryTimer][1]
	require.True(t, gen2Task.GetVisibilityTime().Equal(t3))

	// Txn 3 [t=t1]: executeChasmSideEffectTask (InvokerExecuteTask for action1).
	invoker, err := chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (*scheduler.Invoker, error) {
			return s.Invoker.Get(ctx), nil
		}, struct{}{})
	require.NoError(t, err)
	dropped, err := chasmtest.ExecuteSideEffectTask(t.Context(), testEngine, invoker,
		invokerExecuteHandler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped)

	var requestID1 string
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, start := range s.Invoker.Get(ctx).BufferedStarts {
				if start.GetRunId() == "run-1" {
					requestID1 = start.GetRequestId()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotEmpty(t, requestID1)

	// -- time: t1 → t2 --
	// Txn 4 [t=t2]: CompleteNexusOperationChasm (workflow 1 completes).
	// generator.Generate() fires inline: not idle (remaining=1), so it
	// schedules a duplicate timer at t3 alongside gen2. Both at t3, both timers.
	ts.Update(t2)
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			return nil, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: requestID1,
				CloseTime: timestamppb.New(ts.Now()),
				Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
			})
		}, struct{}{})
	require.NoError(t, err)
	// Acceptable: may be 1 or 2 GeneratorTasks (duplicate timer at t3); all must be GeneratorTask.
	pendingAfterAction1, err := testEngine.PendingPureTaskTypeNames(rootRef)
	require.NoError(t, err)
	require.NotEmpty(t, pendingAfterAction1)
	for _, name := range pendingAfterAction1 {
		require.Contains(t, name, "GeneratorTask")
	}

	// -- time: t2 → t3 --
	// Txn 5 [t=t3]: executeChasmPureTimers (gen2 + any duplicate, both at t3).
	// gen2 processes [t1, t3], finds action2. nextWakeupTime=zero (no more calendar
	// entries) → isIdle=true → schedules SchedulerIdleTask.
	// InvokerProcessBufferTask fires inline, schedules InvokerExecuteTask.
	// The duplicate gen (from Txn 4) also fires and also goes idle — acceptable.
	ts.Update(t3)
	err = testEngine.ExecuteChasmPureTimers(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)
	// Only SchedulerIdleTask(s) pending; no more generator tasks.
	pendingAfterGen2, err := testEngine.PendingPureTaskTypeNames(rootRef)
	require.NoError(t, err)
	require.NotEmpty(t, pendingAfterGen2)
	for _, name := range pendingAfterGen2 {
		require.Contains(t, name, "SchedulerIdleTask")
	}

	// Txn 6 [t=t3]: executeChasmSideEffectTask (InvokerExecuteTask for action2).
	// Sets start2.StartTime=time.Now(); closeTransactionCleanupInvalidTasks drops
	// the idle task(s) scheduled at start1+idleTime (idleExpiration shifted to
	// start2+idleTime ≠ start1+idleTime).
	invoker, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (*scheduler.Invoker, error) {
			return s.Invoker.Get(ctx), nil
		}, struct{}{})
	require.NoError(t, err)
	dropped, err = chasmtest.ExecuteSideEffectTask(t.Context(), testEngine, invoker,
		invokerExecuteHandler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped)

	var requestID2 string
	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			for _, start := range s.Invoker.Get(ctx).BufferedStarts {
				if start.GetRunId() == "run-2" {
					requestID2 = start.GetRequestId()
				}
			}
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
	require.NotEmpty(t, requestID2)

	// -- time: t3 → t4 --
	// Txn 7 [t=t4]: CompleteNexusOperationChasm (workflow 2 completes).
	// generator.Generate() fires inline: nextWakeupTime=zero → isIdle=true →
	// reschedules SchedulerIdleTask at start2.StartTime+idleTime (wall clock).
	ts.Update(t4)
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (chasm.NoValue, error) {
			return nil, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: requestID2,
				CloseTime: timestamppb.New(ts.Now()),
				Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{}},
			})
		}, struct{}{})
	require.NoError(t, err)
	requirePendingTypes(t, testEngine, rootRef, "SchedulerIdleTask")

	// Read the idle task's physical timer — anchored to wall-clock start2.StartTime.
	allTasksAfterAction2, err := testEngine.Tasks(rootRef)
	require.NoError(t, err)
	idleVisibility := allTasksAfterAction2[historytasks.CategoryTimer][len(allTasksAfterAction2[historytasks.CategoryTimer])-1].GetVisibilityTime()

	// -- time: t4 → idleVisibility --
	// Txn 8: executeChasmPureTimers (SchedulerIdleTask fires).
	ts.Update(idleVisibility)
	err = testEngine.ExecuteChasmPureTimers(t.Context(), rootRef, ts.Now())
	require.NoError(t, err)

	sched, err := chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, _ chasm.Context, _ struct{}) (*scheduler.Scheduler, error) {
			return s, nil
		}, struct{}{})
	require.NoError(t, err)
	require.True(t, sched.Closed)
	completed, err := testEngine.IsExecutionCompleted(rootRef)
	require.NoError(t, err)
	require.True(t, completed)
}
