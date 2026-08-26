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
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
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

	// Check that workflow ID lookup now returns empty (request completed)
	require.Empty(t, invoker.RunningWorkflowID(tc.completion.RequestId))

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

func TestHandleNexusCompletion_ExistingAllowAllDoesNotUpdateCompletionState(t *testing.T) {
	for _, tc := range []struct {
		name   string
		manual bool
	}{
		{name: "generator", manual: false},
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
			invoker := sched.Invoker.Get(readCtx)
			require.Empty(t, invoker.GetBufferedStarts())
			require.Len(t, sched.Info.GetRecentActions(), 1)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_FAILED, sched.Info.GetRecentActions()[0].GetStartWorkflowStatus())
		})
	}
}

func TestHandleNexusCompletion_IgnoredReasonMetric(t *testing.T) {
	for _, tc := range []struct {
		name   string
		setup  func(*scheduler.Invoker)
		reason string
	}{
		{
			name:   "unrecognized request",
			reason: "unrecognized_request_id",
		},
		{
			name: "already completed",
			setup: func(invoker *scheduler.Invoker) {
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					RequestId: "request-id",
					Completed: &schedulespb.CompletedResult{
						Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
					},
				}}
			},
			reason: "already_completed",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := metricstest.NewCaptureHandler()
			capture := recorder.StartCapture()
			defer recorder.StopCapture(capture)

			logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
			_, engineCtx := newTestEngineContext(t, logger, withEngineMetricsHandler(recorder))
			_, err := chasm.StartExecution(
				engineCtx,
				chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
				scheduler.CreateScheduler,
				&schedulerpb.CreateScheduleRequest{
					NamespaceId: namespaceID,
					FrontendRequest: &workflowservice.CreateScheduleRequest{
						Namespace: namespace, ScheduleId: scheduleID, Schedule: defaultSchedule(), RequestId: "create-request",
					},
				},
			)
			require.NoError(t, err)
			rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID})

			_, _, err = chasm.UpdateComponent(engineCtx, rootRef,
				func(s *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
					if tc.setup != nil {
						tc.setup(s.Invoker.Get(ctx))
					}
					return struct{}{}, s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{RequestId: "request-id"})
				}, struct{}{})
			require.NoError(t, err)

			recordings := capture.Snapshot()[metrics.ScheduleCallbackIgnored.Name()]
			require.Len(t, recordings, 1)
			require.Equal(t, tc.reason, recordings[0].Tags["reason"])
		})
	}
}

func TestHandleNexusCompletion_MigratedRunningWorkflowKeepsCompletionState(t *testing.T) {
	now := time.Now().UTC()
	v1Schedule := defaultSchedule()
	v1Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	v1Schedule.Policies.PauseOnFailure = true
	v1Info := &schedulepb.ScheduleInfo{
		RunningWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "wf-tracked", RunId: "run-tracked"}},
	}
	v1State := &schedulespb.InternalState{
		Namespace:         namespace,
		NamespaceId:       namespaceID,
		ScheduleId:        scheduleID,
		ConflictToken:     42,
		LastProcessedTime: timestamppb.New(now),
	}
	req := migration.LegacyToCreateFromMigrationStateRequest(v1Schedule, v1Info, v1State, nil, nil, now)

	var migrated *schedulespb.BufferedStart
	for _, start := range req.GetState().GetInvokerState().GetBufferedStarts() {
		if start.GetWorkflowId() == "wf-tracked" {
			migrated = start
			break
		}
	}
	require.NotNil(t, migrated)
	require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_UNSPECIFIED, migrated.GetOverlapPolicy())

	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	_, engineCtx := newTestEngineContext(t, logger)
	handler := scheduler.NewTestHandler(logger)
	_, err := handler.TestCreateFromMigrationState(engineCtx, req)
	require.NoError(t, err)

	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](chasm.ExecutionKey{
		NamespaceID: namespaceID,
		BusinessID:  scheduleID,
	})
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
			require.Equal(t, "tracked workflow failed", s.LastCompletionResult.Get(ctx).GetFailure().GetMessage())
			require.True(t, s.Schedule.State.Paused)
			return struct{}{}, nil
		}, struct{}{})
	require.NoError(t, err)
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
