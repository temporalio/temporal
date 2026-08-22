package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/testing/testlogger"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nexusCompletionTestCase struct {
	name              string
	setupInvoker      func(*scheduler.Invoker)
	setupScheduler    func(*scheduler.Scheduler)
	completion        *persistencespb.ChasmNexusCompletion
	expectPaused      bool
	expectStatus      enumspb.WorkflowExecutionStatus
	expectNoOp        bool
	validateInvoker   func(*testing.T, *scheduler.Invoker)
	validateScheduler func(*testing.T, *scheduler.Scheduler, chasm.Context)
}

func executeNexusCompletion(t *testing.T, tc nexusCompletionTestCase) {
	sched, ctx, node := setupSchedulerForTest(t)

	invoker := sched.Invoker.Get(ctx)

	if tc.setupInvoker != nil {
		tc.setupInvoker(invoker)
	}
	if tc.setupScheduler != nil {
		tc.setupScheduler(sched)
	}

	initialLastCompletion := sched.LastCompletionResult.Get(ctx)

	err := sched.HandleNexusCompletion(ctx, tc.completion)
	require.NoError(t, err)

	_, err = node.CloseTransaction()
	require.NoError(t, err)

	readCtx := chasm.NewContext(context.Background(), node)

	if tc.expectNoOp {
		currentLastCompletion := sched.LastCompletionResult.Get(readCtx)
		require.Equal(t, initialLastCompletion, currentLastCompletion)
		return
	}

	lastCompletion := sched.LastCompletionResult.Get(readCtx)
	require.NotNil(t, lastCompletion)

	if tc.completion.GetSuccess() != nil {
		require.NotNil(t, lastCompletion.GetSuccess())
	} else if tc.completion.GetFailure() != nil {
		require.NotNil(t, lastCompletion.GetFailure())
	}

	require.Equal(t, tc.expectPaused, sched.Schedule.State.Paused)
	if tc.expectPaused {
		require.NotEmpty(t, sched.Schedule.State.Notes)
		require.Contains(t, sched.Schedule.State.Notes, "wf-1")
	}

	// Check that the BufferedStart HandleNexusCompletion actually matched (by
	// RequestId, mirroring getBufferedStart) is now marked completed. This
	// exercises the real completion path directly rather than runningWorkflowID,
	// which HandleNexusCompletion no longer calls in production.
	matched := false
	for _, start := range invoker.BufferedStarts {
		if start.GetRequestId() == tc.completion.RequestId {
			matched = true
			require.NotNil(t, start.GetCompleted(),
				"expected BufferedStart for RequestId %s to be marked completed", tc.completion.RequestId)
			break
		}
	}
	require.True(t, matched, "expected to find a BufferedStart for RequestId %s", tc.completion.RequestId)

	// If we expect a specific status, verify the BufferedStart has Completed set
	if tc.expectStatus != enumspb.WORKFLOW_EXECUTION_STATUS_UNSPECIFIED {
		found := false
		for _, start := range invoker.BufferedStarts {
			if start.GetWorkflowId() == "wf-1" && start.GetCompleted() != nil {
				require.Equal(t, tc.expectStatus, start.GetCompleted().GetStatus())
				found = true
				break
			}
		}
		require.True(t, found, "expected to find completed BufferedStart with workflow ID wf-1")
	}

	if tc.validateInvoker != nil {
		tc.validateInvoker(t, invoker)
	}
	if tc.validateScheduler != nil {
		tc.validateScheduler(t, sched, readCtx)
	}
}

// TestHandleNexusCompletion_Success verifies that a successful workflow completion
// is properly recorded with the success payload and workflow status is updated.
func TestHandleNexusCompletion_Success(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "successful completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}

	executeNexusCompletion(t, tc)
}

func TestHandleNexusCompletion_ExistingAllowAllStoresRecentAction(t *testing.T) {
	for _, tc := range []struct {
		name   string
		manual bool
	}{
		{name: "generator"},
		{name: "backfiller", manual: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sched, ctx, node := setupSchedulerForTest(t)
			initial := &schedulerpb.LastCompletionResult{Success: &commonpb.Payload{Data: []byte("previous-result")}}
			sched.LastCompletionResult = chasm.NewDataField(ctx, initial)
			sched.Schedule.Policies.PauseOnFailure = true
			sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
				RequestId: "req-1", WorkflowId: "wf-1", RunId: "run-1", Attempt: 1,
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				ActualTime:    timestamppb.New(time.Now().Add(-time.Minute)),
				StartTime:     timestamppb.New(time.Now().Add(-30 * time.Second)),
				Manual:        tc.manual,
			}}

			err := sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: "req-1",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "allow-all failure"},
				},
				CloseTime: timestamppb.Now(),
			})
			require.NoError(t, err)
			_, err = node.CloseTransaction()
			require.NoError(t, err)

			readCtx := chasm.NewContext(context.Background(), node)
			require.Equal(t, initial, sched.LastCompletionResult.Get(readCtx))
			require.False(t, sched.Schedule.State.Paused)
			require.Empty(t, sched.Invoker.Get(readCtx).GetBufferedStarts())
			require.Len(t, sched.Info.GetRecentActions(), 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				sched.Info.GetRecentActions()[0].GetStartWorkflowStatus())
		})
	}
}

// TestHandleNexusCompletion_AllowAllDoesNotUpdateCompletionState drives a real
// CHASM engine end to end (create -> mutate+complete -> read) rather than a
// hand-wired node, so the assertion exercises the same component-ref and
// transaction boundaries a production nexus completion callback does.
func TestHandleNexusCompletion_AllowAllDoesNotUpdateCompletionState(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	_, engineCtx := newTestEngineContext(t, logger)

	_, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
		scheduler.CreateScheduler,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Schedule:   defaultSchedule(),
				RequestId:  "req-create",
			},
		},
	)
	require.NoError(t, err)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})

	now := time.Now()
	initial := &schedulerpb.LastCompletionResult{Success: &commonpb.Payload{Data: []byte("previous-result")}}
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
			s.Schedule.Policies.PauseOnFailure = true
			s.LastCompletionResult = chasm.NewDataField(ctx, initial)
			s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
				RequestId: "req-1", WorkflowId: "wf-1", RunId: "run-1", Attempt: 1,
				// Stamped when the action was buffered, as production does.
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				ActualTime:    timestamppb.New(now.Add(-time.Minute)),
				StartTime:     timestamppb.New(now.Add(-30 * time.Second)),
			}}
			return struct{}{}, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: "req-1",
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "allow-all failure"},
				},
				CloseTime: timestamppb.New(now),
			})
		}, struct{}{})
	require.NoError(t, err)

	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Equal(t, initial, s.LastCompletionResult.Get(ctx))
			require.False(t, s.Schedule.State.Paused)
			require.Empty(t, s.Invoker.Get(ctx).GetBufferedStarts())
			require.Len(t, s.Info.GetRecentActions(), 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
				s.Info.GetRecentActions()[0].GetStartWorkflowStatus())
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
}

// TestHandleNexusCompletion_AllowAllReadsStartsOwnPolicy proves
// HandleNexusCompletion reads ALLOW_ALL from the BufferedStart's own stamped
// OverlapPolicy and never from the schedule's current live policy. Production
// stamps a concretely-resolved policy onto every BufferedStart at buffer time
// (spec_processor.go, backfiller_tasks.go), so the tracking decision is frozen
// with the action and a later UpdateSchedule cannot retroactively change how an
// already-started action's completion is handled. The schedule's own policy is
// left at its default (Skip) here specifically to catch a regression that
// resolves against live state, which would otherwise pass by coincidence.
func TestHandleNexusCompletion_AllowAllReadsStartsOwnPolicy(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)
	sched.Schedule.Policies.PauseOnFailure = true // schedule.Policies.OverlapPolicy stays at its Skip default.

	initial := &schedulerpb.LastCompletionResult{
		Success: &commonpb.Payload{Data: []byte("previous-result")},
		Failure: &failurepb.Failure{Message: "previous-failure"},
	}
	sched.LastCompletionResult = chasm.NewDataField(ctx, initial)
	sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:     "req-1",
		WorkflowId:    "wf-1",
		RunId:         "run-1",
		Attempt:       1,
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, // stamped at buffer time, per production.
		ActualTime:    timestamppb.New(time.Now().Add(-time.Minute)),
		StartTime:     timestamppb.New(time.Now().Add(-30 * time.Second)),
	}}

	err := sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "req-1",
		Outcome: &persistencespb.ChasmNexusCompletion_Failure{
			Failure: &failurepb.Failure{Message: "allow-all failure"},
		},
		CloseTime: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)

	_, err = node.CloseTransaction()
	require.NoError(t, err)

	readCtx := chasm.NewContext(context.Background(), node)
	require.Equal(t, initial, sched.LastCompletionResult.Get(readCtx))
	require.False(t, sched.Schedule.State.Paused)
	require.Empty(t, sched.Invoker.Get(readCtx).GetBufferedStarts())
	require.Len(t, sched.Info.GetRecentActions(), 1)
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		sched.Info.GetRecentActions()[0].GetStartWorkflowStatus())
}

// TestHandleNexusCompletion_MigratedRunningWorkflowKeepsCompletionState is a
// regression test for the V1-parity gap that the ALLOW_ALL completion-state
// exclusion opens up on the migration path.
//
// V1 excludes ALLOW_ALL runs from sequential completion state purely by *not
// tracking them*: recordAction only appends a start to Info.RunningWorkflows
// when it was the non-overlapping start (workflow.go), and processWatcherResult
// then applies pause-on-failure and updates LastCompletionResult for whatever
// is in that list, with no overlap-policy check of its own. So a workflow's
// presence in Info.RunningWorkflows is itself proof V1 was tracking it. The
// V2-to-V1 direction already relies on exactly this invariant -- see
// TestCHASMToLegacyStartScheduleArgs_ExcludesAllowAllFromRunningWorkflows in
// the migration package.
//
// The V1-to-V2 direction, however, converts Info.RunningWorkflows into
// BufferedStarts *without* stamping an overlap policy, so those starts carry
// UNSPECIFIED. resolveOverlapPolicy then resolves UNSPECIFIED against the
// schedule's current live policy, which means a tracked workflow on a schedule
// whose live policy happens to be ALLOW_ALL now silently loses its completion
// result and skips pause-on-failure -- the inverse of the divergence this
// change set out to fix, and a regression against both V1 and pre-change V2.
//
// This needs no policy mutation to reach: a backfill or trigger carrying an
// explicit non-ALLOW_ALL overlap policy against an ALLOW_ALL schedule resolves
// to non-ALLOW_ALL, becomes the non-overlapping start, and is tracked.
func TestHandleNexusCompletion_MigratedRunningWorkflowKeepsCompletionState(t *testing.T) {
	now := time.Now().UTC()

	// V1 state: live policy is ALLOW_ALL, but a workflow sits in
	// Info.RunningWorkflows, so V1 was watching it and would act on its outcome.
	v1Schedule := defaultSchedule()
	v1Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	v1Schedule.Policies.PauseOnFailure = true
	v1Info := &schedulepb.ScheduleInfo{
		RunningWorkflows: []*commonpb.WorkflowExecution{
			{WorkflowId: "wf-tracked", RunId: "run-tracked"},
		},
	}
	v1State := &schedulespb.InternalState{
		Namespace:         namespace,
		NamespaceId:       namespaceID,
		ScheduleId:        scheduleID,
		ConflictToken:     42,
		LastProcessedTime: timestamppb.New(now),
	}

	// Run the real conversion rather than hand-rolling its output, so this test
	// keeps tracking migration's actual behavior.
	req := migration.LegacyToCreateFromMigrationStateRequest(v1Schedule, v1Info, v1State, nil, nil, now)

	var migrated *schedulespb.BufferedStart
	for _, start := range req.GetState().GetInvokerState().GetBufferedStarts() {
		if start.GetWorkflowId() == "wf-tracked" {
			migrated = start
			break
		}
	}
	require.NotNil(t, migrated, "migration must convert the running workflow into a BufferedStart")
	require.NotEmpty(t, migrated.GetRequestId(), "the converted start needs a request ID to match a completion")
	// Precondition, asserted so a future migration fix that stamps a concrete
	// policy shows up here as an intentional change rather than a silent pass.
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, migrated.GetOverlapPolicy(),
		"convertRunningWorkflowsToBufferedStarts stamps no overlap policy")

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	_, engineCtx := newTestEngineContext(t, logger)
	handler := scheduler.NewTestHandler(logger)
	_, err := handler.TestCreateFromMigrationState(engineCtx, req)
	require.NoError(t, err)

	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})

	// The workflow V1 was tracking fails after the migration lands.
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: migrated.GetRequestId(),
				Outcome: &persistencespb.ChasmNexusCompletion_Failure{
					Failure: &failurepb.Failure{Message: "tracked workflow failed"},
				},
				CloseTime: timestamppb.New(now),
			})
		}, struct{}{})
	require.NoError(t, err)

	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.Equal(t, "tracked workflow failed",
				s.LastCompletionResult.Get(ctx).GetFailure().GetMessage(),
				"a workflow V1 tracked in RunningWorkflows must still record its failure after migration")
			require.True(t, s.Schedule.State.Paused,
				"pause-on-failure must still apply to a workflow V1 was tracking")
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
}

// TestHandleNexusCompletion_LateStragglerAfterCloseIsNotBlocked documents a
// deliberate, temporary tradeoff: a schedule can close (Delete, migration to
// V1, idle-task quiescence) while a start it made is still running, since none
// of those paths drain BufferedStarts first. That start's completion then
// arrives as a late straggler. HandleNexusCompletion does not reject it on
// s.Closed today: an ALLOW_ALL schedule is expected to have completions arrive
// after it closes, and a blanket guard would lock those out forever along with
// the genuinely-stray ones. Distinguishing the two needs a more considered
// fix than "if s.Closed return err" — tracked for a follow-up PR. For now this
// pins down that a late straggler is still processed rather than silently
// swallowed or erroring.
func TestHandleNexusCompletion_LateStragglerAfterCloseIsNotBlocked(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	_, engineCtx := newTestEngineContext(t, logger)

	_, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
		scheduler.CreateScheduler,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Schedule:   defaultSchedule(),
				RequestId:  "req-create",
			},
		},
	)
	require.NoError(t, err)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})

	now := time.Now()
	initial := &schedulerpb.LastCompletionResult{Success: &commonpb.Payload{Data: []byte("previous-result")}}

	// First transaction: a still-running start, then the schedule closes out from
	// under it (mirrors Delete/migration/idle-quiescence, none of which drain
	// BufferedStarts before closing).
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			s.LastCompletionResult = chasm.NewDataField(ctx, initial)
			s.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
				RequestId: "req-1", WorkflowId: "wf-1", RunId: "run-1", Attempt: 1,
				ActualTime: timestamppb.New(now.Add(-time.Minute)),
				StartTime:  timestamppb.New(now.Add(-30 * time.Second)),
			}}
			s.Closed = true
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)

	// Second, separate transaction: the straggling start's completion arrives after
	// the close committed. The ref still resolves (the engine only validates
	// ancestor lifecycle, never the target's own), so nothing upstream stops this
	// from reaching HandleNexusCompletion.
	_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			return struct{}{}, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: "req-1",
				Outcome: &persistencespb.ChasmNexusCompletion_Success{
					Success: &commonpb.Payload{Data: []byte("late-straggler-result")},
				},
				CloseTime: timestamppb.New(now),
			})
		}, struct{}{})
	require.NoError(t, err)

	_, err = chasm.ReadComponent(engineCtx, rootRef,
		func(s *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			require.NotEqual(t, initial, s.LastCompletionResult.Get(ctx))
			require.NotNil(t, s.LastCompletionResult.Get(ctx).GetSuccess())
			require.NotNil(t, s.Invoker.Get(ctx).BufferedStarts[0].GetCompleted())
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
}

// TestHandleNexusCompletion_AlreadyCompletedIsIgnored verifies that a
// duplicate delivery of a completion for a BufferedStart that's already
// recorded as completed is fast-succeeded without mutating state again,
// distinctly from the unrecognized-request-ID case (both fast-succeed, but
// for different reasons — see the ScheduleCallbackIgnored ReasonTag).
func TestHandleNexusCompletion_AlreadyCompletedIsIgnored(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)
	sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId: "req-1", WorkflowId: "wf-1", RunId: "run-1", Attempt: 1,
		ActualTime: timestamppb.New(time.Now().Add(-time.Minute)),
		StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
		Completed: &schedulespb.CompletedResult{
			Status:    enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			CloseTime: timestamppb.New(time.Now().Add(-15 * time.Second)),
		},
	}}
	initial := &schedulerpb.LastCompletionResult{Success: &commonpb.Payload{Data: []byte("previous-result")}}
	sched.LastCompletionResult = chasm.NewDataField(ctx, initial)

	// A second delivery of the same completion, now that the start is already
	// marked completed, must not touch LastCompletionResult again.
	err := sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "req-1",
		Outcome: &persistencespb.ChasmNexusCompletion_Success{
			Success: &commonpb.Payload{Data: []byte("duplicate-delivery")},
		},
		CloseTime: timestamppb.New(time.Now()),
	})
	require.NoError(t, err)

	_, err = node.CloseTransaction()
	require.NoError(t, err)

	readCtx := chasm.NewContext(context.Background(), node)
	require.Equal(t, initial, sched.LastCompletionResult.Get(readCtx))
}

// TestHandleNexusCompletion_Failure verifies that a failed workflow completion
// is properly recorded with the failure payload and workflow status is updated.
func TestHandleNexusCompletion_Failure(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "failed completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_PauseOnFailure verifies that when PauseOnFailure is enabled,
// a workflow failure causes the schedule to be paused and notes to be set.
func TestHandleNexusCompletion_PauseOnFailure(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "pause on failure",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		setupScheduler: func(sched *scheduler.Scheduler) {
			sched.Schedule.Policies.PauseOnFailure = true
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow failed",
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: true,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
	}

	executeNexusCompletion(t, tc)
}

func TestPauseOnFailureInvalidatesConflictToken(t *testing.T) {
	sched, ctx, node := setupSchedulerForTest(t)
	sched.Schedule.Policies.PauseOnFailure = true
	sched.Invoker.Get(ctx).BufferedStarts = []*schedulespb.BufferedStart{
		{
			RequestId:  "req-1",
			WorkflowId: "wf-1",
			RunId:      "run-1",
			Attempt:    1,
		},
	}
	_, err := node.CloseTransaction()
	require.NoError(t, err)

	describeResponse, err := sched.Describe(
		chasm.NewContext(context.Background(), node),
		&schedulerpb.DescribeScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.DescribeScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
			},
		},
		newLegacySpecBuilder(0, 0),
	)
	require.NoError(t, err)
	staleDescription := describeResponse.GetFrontendResponse()
	require.False(t, staleDescription.GetSchedule().GetState().GetPaused())

	ctx = chasm.NewMutableContext(context.Background(), node)
	err = sched.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
		RequestId: "req-1",
		Outcome: &persistencespb.ChasmNexusCompletion_Failure{
			Failure: &failurepb.Failure{Message: "workflow failed"},
		},
		CloseTime: timestamppb.Now(),
	})
	require.NoError(t, err)
	_, err = node.CloseTransaction()
	require.NoError(t, err)
	require.True(t, sched.Schedule.GetState().GetPaused())
	require.NotEmpty(t, sched.Schedule.GetState().GetNotes())

	ctx = chasm.NewMutableContext(context.Background(), node)
	_, err = sched.Update(ctx, &schedulerpb.UpdateScheduleRequest{
		NamespaceId: namespaceID,
		FrontendRequest: &workflowservice.UpdateScheduleRequest{
			Namespace:     namespace,
			ScheduleId:    scheduleID,
			Schedule:      staleDescription.GetSchedule(),
			ConflictToken: staleDescription.GetConflictToken(),
		},
	})
	require.ErrorIs(t, err, scheduler.ErrConflictTokenMismatch)
	require.True(t, sched.Schedule.GetState().GetPaused())
	require.NotEmpty(t, sched.Schedule.GetState().GetNotes())
}

// TestHandleNexusCompletion_Idempotent verifies that handling a completion for an
// already-processed request ID (not in BufferedStarts) is a no-op.
func TestHandleNexusCompletion_Idempotent(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "idempotent completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			// Empty BufferedStarts - request was already processed
			invoker.BufferedStarts = []*schedulespb.BufferedStart{}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectNoOp: true,
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_Canceled verifies that a canceled workflow completion
// is properly recorded with CANCELED status.
func TestHandleNexusCompletion_Canceled(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "canceled completion",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Failure{
				Failure: &failurepb.Failure{
					Message: "workflow canceled",
					FailureInfo: &failurepb.Failure_CanceledFailureInfo{
						CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
					},
				},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
	}

	executeNexusCompletion(t, tc)
}

// Deferred starts (Attempt==-1, set by ProcessBuffer when overlap policy
// holds them back) must be re-enabled when a running workflow completes.
// recordCompletedAction flips -1 to 0; the immediate ProcessBufferTask
// addTasks emits fires inline during CloseTransaction and promotes 0 to 1.
// End state Attempt=1 demonstrates the full defer -> re-enable -> promote
// cascade.
func TestHandleNexusCompletion_ReenablesDeferredStarts(t *testing.T) {
	tc := nexusCompletionTestCase{
		name: "completion re-enables deferred starts",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:  "req-1",
					WorkflowId: "wf-1",
					RunId:      "run-1",
					Attempt:    1,
					ActualTime: timestamppb.New(time.Now().Add(-1 * time.Minute)),
					StartTime:  timestamppb.New(time.Now().Add(-30 * time.Second)),
				},
				{
					RequestId:  "req-2",
					WorkflowId: "wf-2",
					Attempt:    -1,
					ActualTime: timestamppb.New(time.Now()),
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("ok")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		validateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			var deferred *schedulespb.BufferedStart
			for _, start := range invoker.BufferedStarts {
				if start.RequestId == "req-2" {
					deferred = start
					break
				}
			}
			require.NotNil(t, deferred, "previously-deferred start must remain in the buffer")
			require.Equal(t, int64(1), deferred.Attempt,
				"deferred start must be re-enabled past 0 (recordCompletedAction) and promoted to exactly 1 (inline ProcessBufferTask)")
		},
	}

	executeNexusCompletion(t, tc)
}

// TestHandleNexusCompletion_CompletionBeforeStart verifies that a workflow can
// complete before its start is recorded (workflow has a BufferedStart but no RunId yet).
func TestHandleNexusCompletion_CompletionBeforeStart(t *testing.T) {
	desiredTime := time.Now()
	tc := nexusCompletionTestCase{
		name: "completion before start",
		setupInvoker: func(invoker *scheduler.Invoker) {
			invoker.BufferedStarts = []*schedulespb.BufferedStart{
				{
					RequestId:   "req-1",
					WorkflowId:  "wf-1",
					Attempt:     1,
					ActualTime:  timestamppb.New(desiredTime),
					DesiredTime: timestamppb.New(desiredTime),
					// No RunId - workflow hasn't been started yet in our records
				},
			}
		},
		completion: &persistencespb.ChasmNexusCompletion{
			RequestId: "req-1",
			Outcome: &persistencespb.ChasmNexusCompletion_Success{
				Success: &commonpb.Payload{Data: []byte("success-data")},
			},
			CloseTime: timestamppb.New(time.Now()),
		},
		expectPaused: false,
		expectStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		validateInvoker: func(t *testing.T, invoker *scheduler.Invoker) {
			require.Len(t, invoker.BufferedStarts, 1)
			require.NotNil(t, invoker.BufferedStarts[0].Completed)
		},
	}

	executeNexusCompletion(t, tc)
}
