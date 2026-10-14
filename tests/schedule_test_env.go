package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/service/worker/scheduler"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// contextFactory wraps a base context for CHASM vs V1 differences.
type contextFactory func(context.Context) context.Context

var (
	chasmContextFactory contextFactory = func(ctx context.Context) context.Context {
		return metadata.NewOutgoingContext(ctx, metadata.Pairs(
			headers.ExperimentHeaderName, "chasm-scheduler",
		))
	}
	v1ContextFactory contextFactory = func(ctx context.Context) context.Context {
		return ctx
	}
)

// completeSignalName releases a workflow registered via registerGatedWorkflow.
const completeSignalName = "complete"

type ScheduleTestEnv struct {
	*testcore.TestEnv
}

func newScheduleEnv(t *testing.T, opts ...testcore.TestOption) *ScheduleTestEnv {
	t.Helper()
	opts = append(opts, testcore.WithDynamicConfig(dynamicconfig.FrontendAllowedExperiments, []string{"*"}))
	env := testcore.NewEnv(t, opts...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(chasmContextFactory(testcore.NewContext()), 30*time.Second)
		defer cancel()

		var scheduleIDs []string
		var nextPageToken []byte
		for {
			response, err := env.FrontendClient().ListSchedules(ctx, &workflowservice.ListSchedulesRequest{
				Namespace:       env.Namespace().String(),
				MaximumPageSize: 1000,
				NextPageToken:   nextPageToken,
			})
			if err != nil {
				if t.Failed() {
					t.Logf("schedule cleanup failed: list schedules: %v", err)
				} else {
					t.Errorf("schedule cleanup failed: list schedules: %v", err)
				}
				return
			}
			for _, schedule := range response.GetSchedules() {
				scheduleIDs = append(scheduleIDs, schedule.GetScheduleId())
			}
			nextPageToken = response.GetNextPageToken()
			if len(nextPageToken) == 0 {
				break
			}
		}

		var cleanupErr error
		for _, scheduleID := range scheduleIDs {
			_, err := env.FrontendClient().DeleteSchedule(ctx, &workflowservice.DeleteScheduleRequest{
				Namespace:  env.Namespace().String(),
				ScheduleId: scheduleID,
				Identity:   "test cleanup",
			})
			var notFoundErr *serviceerror.NotFound
			if err != nil && !errors.As(err, &notFoundErr) {
				cleanupErr = errors.Join(cleanupErr, fmt.Errorf("delete schedule %q: %w", scheduleID, err))
			}
		}
		if cleanupErr != nil {
			if t.Failed() {
				t.Logf("schedule cleanup failed: %v", cleanupErr)
			} else {
				t.Errorf("schedule cleanup failed: %v", cleanupErr)
			}
		}
	})
	return &ScheduleTestEnv{TestEnv: env}
}

// requireNoChasmSentinel asserts that no CHASM entity -- sentinel or otherwise
// -- exists yet for scheduleID under the scheduler archetype. Used right after
// creating a V1 schedule to confirm the test's "no sentinel gets written"
// assumption directly (rather than only inferring it from the EnableChasm
// value passed to CreateSchedule), since a stray/unexpired sentinel would
// invisibly gate migration behind chasm/lib/scheduler/config.go's
// SentinelIdleTime (15 minutes) and make an otherwise-passing test hang or
// flake for the wrong reason.
func (env *ScheduleTestEnv) requireNoChasmSentinel(ctx context.Context, t *testing.T, scheduleID string) {
	t.Helper()

	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: scheduleID},
		Archetype: string(chasm.SchedulerArchetype),
	})
	if err != nil {
		// NotFound means nothing at all was written to the CHASM key space for
		// this schedule ID yet -- definitely no sentinel. Any other error is
		// unexpected and should fail the test.
		var notFoundErr *serviceerror.NotFound
		require.ErrorAs(t, err, &notFoundErr, "unexpected error checking for a CHASM sentinel")
		return
	}

	node := resp.GetDatabaseMutableState().GetChasmNodes()[""]
	require.NotNil(t, node, "CHASM execution exists for %q but has no root node", scheduleID)

	var state schedulerpb.SchedulerState
	require.NoError(t, proto.Unmarshal(node.GetData().GetData(), &state))
	require.False(t, state.GetSentinel(),
		"a CHASM sentinel exists for schedule %q -- it would block migration for up to "+
			"SentinelIdleTime (chasm/lib/scheduler/config.go), invalidating this test's timing", scheduleID)
}

// createV1Schedule creates a V1 (workflow-backed) schedule with CHASM disabled,
// then asserts no CHASM sentinel was written. initialPatch may be nil.
func (env *ScheduleTestEnv) createV1Schedule(
	ctx context.Context,
	t *testing.T,
	scheduleID string,
	sched *schedulepb.Schedule,
	initialPatch *schedulepb.SchedulePatch,
) {
	t.Helper()

	// EnableChasm is unset at creation time, so no CHASM sentinel gets written (which would block migration).
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)

	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:    env.Namespace().String(),
		ScheduleId:   scheduleID,
		Schedule:     sched,
		InitialPatch: initialPatch,
		Identity:     "test",
		RequestId:    uuid.NewString(),
	})
	require.NoError(t, err)
	env.requireNoChasmSentinel(ctx, t, scheduleID)
}

// awaitRunningAction waits until the schedule has fired an action whose workflow
// is still RUNNING, and returns that workflow's ID.
func (env *ScheduleTestEnv) awaitRunningAction(ctx context.Context, t *testing.T, scheduleID string) string {
	t.Helper()

	var runningWfID string
	await.RequireTrue(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		a := descResp.Info.RecentActions[0]
		if a.GetStartWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return false
		}
		runningWfID = a.GetStartWorkflowResult().GetWorkflowId()
		return true
	}, 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, runningWfID)
	return runningWfID
}

// awaitAnyAction waits until the schedule has fired at least one action
// (regardless of the fired workflow's status) and returns that workflow's ID.
func (env *ScheduleTestEnv) awaitAnyAction(ctx context.Context, t *testing.T, scheduleID string) string {
	t.Helper()

	var wfID string
	await.RequireTrue(t, func() bool {
		descResp, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  env.Namespace().String(),
			ScheduleId: scheduleID,
		})
		if err != nil || len(descResp.GetInfo().GetRecentActions()) == 0 {
			return false
		}
		wfID = descResp.Info.RecentActions[0].GetStartWorkflowResult().GetWorkflowId()
		return wfID != ""
	}, 15*time.Second, 500*time.Millisecond)
	require.NotEmpty(t, wfID)
	return wfID
}

// awaitV1SchedulerCompleted waits until the V1 scheduler workflow reaches
// COMPLETED. The V1 scheduler workflow only completes when executeMigration()
// succeeds, so its completion is a reliable "migration happened" signal.
func (env *ScheduleTestEnv) awaitV1SchedulerCompleted(ctx context.Context, t *testing.T, scheduleID string) {
	t.Helper()

	v1WorkflowID := scheduler.WorkflowIDPrefix + scheduleID
	await.RequireTruef(t, func() bool {
		desc, err := env.GetTestCluster().HistoryClient().DescribeWorkflowExecution(ctx, &historyservice.DescribeWorkflowExecutionRequest{
			NamespaceId: env.NamespaceID().String(),
			Request: &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: env.Namespace().String(),
				Execution: &commonpb.WorkflowExecution{WorkflowId: v1WorkflowID},
			},
		})
		return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED
	}, 30*time.Second, 1*time.Second, "V1 scheduler workflow should complete once migration succeeds")
}

// requireNoOptionsUpdatedEvent asserts the workflow's history does not contain
// EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED. V1 never emits this event, so
// older/community SDKs (whose vendored protobuf predates it) cannot decode it
// and crash -- permanently stalling the workflow (see
// repros/scheduler-migration-bug-evidence.md).
func (env *ScheduleTestEnv) requireNoOptionsUpdatedEvent(ctx context.Context, t *testing.T, workflowID string) {
	t.Helper()

	history, err := env.FrontendClient().GetWorkflowExecutionHistory(ctx, &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	require.NoError(t, err)
	for _, event := range history.GetHistory().GetEvents() {
		require.NotEqual(t, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventType(),
			"workflow %q gained %s (event id %d) during migration -- older SDKs cannot decode this "+
				"event type and will permanently stall on it (see repros/scheduler-migration-bug-evidence.md)",
			workflowID, enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED, event.GetEventId())
	}
}

// requireV2ScheduleExists asserts a V2 (CHASM) schedule exists for scheduleID.
func (env *ScheduleTestEnv) requireV2ScheduleExists(ctx context.Context, t *testing.T, scheduleID string) {
	t.Helper()

	_, err := env.GetTestCluster().SchedulerClient().DescribeSchedule(ctx, &schedulerpb.DescribeScheduleRequest{
		NamespaceId:     env.NamespaceID().String(),
		FrontendRequest: &workflowservice.DescribeScheduleRequest{Namespace: env.Namespace().String(), ScheduleId: scheduleID},
	})
	require.NoError(t, err)
}

// createSchedule creates sched under sid and fails the test on error.
func (env *ScheduleTestEnv) createSchedule(ctx context.Context, t *testing.T, sid string, sched *schedulepb.Schedule) {
	t.Helper()
	_, err := env.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Schedule:   sched,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
}

// patchSchedule applies patch to sid and fails the test on error.
func (env *ScheduleTestEnv) patchSchedule(ctx context.Context, t *testing.T, sid string, patch *schedulepb.SchedulePatch) {
	t.Helper()
	_, err := env.FrontendClient().PatchSchedule(ctx, &workflowservice.PatchScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
		Patch:      patch,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)
}

// startWorkflowAction builds the StartWorkflow action shared by these tests.
func (env *ScheduleTestEnv) startWorkflowAction(wid, wt string) *schedulepb.ScheduleAction {
	return &schedulepb.ScheduleAction{
		Action: &schedulepb.ScheduleAction_StartWorkflow{
			StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
				WorkflowId:            wid,
				WorkflowType:          &commonpb.WorkflowType{Name: wt},
				TaskQueue:             &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			},
		},
	}
}

// registerCountingWorkflow registers a workflow that records each execution in
// runs (via SideEffect, so replays don't double-count) and returns immediately.
//
// Each registered counting workflow should be associated with a distinct `runs`
// atomic.
func (env *ScheduleTestEnv) registerCountingWorkflow(wt string, runs *atomic.Int32) {
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		_ = workflow.SideEffect(ctx, func(workflow.Context) any { runs.Add(1); return 0 })
		return nil
	}, workflow.RegisterOptions{Name: wt})
}

// registerGatedWorkflow is like registerCountingWorkflow but the workflow stays
// running until the test signals completeSignalName (via completeRunningWorkflows).
func (env *ScheduleTestEnv) registerGatedWorkflow(wt string, runs *atomic.Int32) {
	env.SdkWorker().RegisterWorkflowWithOptions(func(ctx workflow.Context) error {
		_ = workflow.SideEffect(ctx, func(workflow.Context) any { runs.Add(1); return 0 })
		workflow.GetSignalChannel(ctx, completeSignalName).Receive(ctx, nil)
		return nil
	}, workflow.RegisterOptions{Name: wt})
}

// scheduleClosed reports whether the schedule has closed, i.e. DescribeSchedule
// returns NotFound specifically (not just any error).
func (env *ScheduleTestEnv) scheduleClosed(ctx context.Context, sid string) bool {
	_, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	var notFound *serviceerror.NotFound
	return errors.As(err, &notFound)
}

// completeRunningWorkflows signals completeSignalName to every running workflow
// of the schedule and returns the number it signaled.
func (env *ScheduleTestEnv) completeRunningWorkflows(ctx context.Context, t *testing.T, sid string) int {
	t.Helper()
	desc, err := env.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  env.Namespace().String(),
		ScheduleId: sid,
	})
	require.NoError(t, err)
	running := desc.GetInfo().GetRunningWorkflows()
	for _, wf := range running {
		_, err := env.FrontendClient().SignalWorkflowExecution(ctx, &workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
			WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: wf.GetWorkflowId()},
			SignalName:        completeSignalName,
			Identity:          "test",
			RequestId:         uuid.NewString(),
		})
		require.NoError(t, err)
	}
	return len(running)
}

// createSchedulerFromMigrationState creates a V2 scheduler directly from a migration
// state carrying a single BufferedStart pointing at (wid, runID). That start is what
// arms the callback re-attach task: it is the only path that produces a start with a
// RunId but no callback attached, so it is the only way to reach
// SchedulerCallbacksTaskHandler in a running server.
func (env *ScheduleTestEnv) createSchedulerFromMigrationState(
	ctx context.Context,
	t *testing.T,
	sid, wid, wt, runID string,
) {
	t.Helper()

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(24 * time.Hour)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
	}

	now := time.Now().UTC()
	nsID := env.NamespaceID().String()

	migrationState := &schedulerpb.SchedulerMigrationState{
		SchedulerState: &schedulerpb.SchedulerState{
			Namespace:     env.Namespace().String(),
			NamespaceId:   nsID,
			ScheduleId:    sid,
			Schedule:      schedule,
			Info:          &schedulepb.ScheduleInfo{},
			ConflictToken: 1,
		},
		GeneratorState: &schedulerpb.GeneratorState{},
		InvokerState: &schedulerpb.InvokerState{
			BufferedStarts: []*schedulespb.BufferedStart{
				{
					NominalTime: timestamppb.New(now),
					ActualTime:  timestamppb.New(now),
					StartTime:   timestamppb.New(now),
					WorkflowId:  wid,
					RunId:       runID,
					RequestId:   uuid.NewString(),
					Attempt:     1,
					HasCallback: false,
				},
			},
		},
	}
	_, err := env.GetTestCluster().SchedulerClient().CreateFromMigrationState(
		ctx,
		&schedulerpb.CreateFromMigrationStateRequest{
			NamespaceId: nsID,
			State:       migrationState,
		},
	)
	require.NoError(t, err)
}

// getScheduleEntryFromVisibility polls visibility using ListSchedules until it finds a schedule
// with the given id and for which the optional predicate function returns true.
func (env *ScheduleTestEnv) getScheduleEntryFromVisibility(t *testing.T, sid string, newContext contextFactory, predicate func(*schedulepb.ScheduleListEntry) bool) *schedulepb.ScheduleListEntry {
	t.Helper()
	var slEntry *schedulepb.ScheduleListEntry
	await.Require(newContext(testcontext.For(t)), t, func(at *await.T) { // wait for visibility
		listResp, err := env.FrontendClient().ListSchedules(at.Context(), &workflowservice.ListSchedulesRequest{
			Namespace:       env.Namespace().String(),
			MaximumPageSize: 5,
		})
		require.NoError(at, err)
		for _, ent := range listResp.Schedules {
			if ent.ScheduleId == sid {
				if predicate != nil {
					require.True(at, predicate(ent), "schedule %q has not reached the expected visibility state", sid)
				}
				slEntry = ent
				return
			}
		}
		require.FailNow(at, "schedule has not appeared in visibility")
	}, 15*time.Second, 1*time.Second)
	return slEntry
}
