package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

var allOverlapPolicies = []enumspb.ScheduleOverlapPolicy{
	enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
	enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
	enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
}

// expectScheduleProgressesWhilePaused returns whether a schedule using the given
// overlap policy continues to take new scheduled actions while the workflow it
// started is paused. This encodes the observed, current behavior of each
// scheduler implementation.
//
//   - ALLOW_ALL never inspects the running workflow, so it always starts new
//     runs regardless of pause (both schedulers).
//   - TERMINATE_OTHER terminates the running workflow (a hard close that does
//     not require the workflow to process a workflow task) and then starts the
//     next run. CHASM does this even when the workflow is paused. The V1
//     scheduler does NOT — it stalls. This V1 stall is a bug: the V1 watcher
//     does not understand the PAUSED status and never observes the paused
//     workflow close, so it never gets to start the buffered run.
//   - SKIP / BUFFER_ONE / BUFFER_ALL / CANCEL_OTHER all keep the slot occupied
//     by the paused workflow, so no new run is taken under either scheduler.
//     (For CANCEL_OTHER the schedule additionally never makes progress on its
//     own because a paused workflow cannot process the cancellation request -
//     it has no workflow task - so the cancel never completes.)
func expectScheduleProgressesWhilePaused(isCHASM bool, policy enumspb.ScheduleOverlapPolicy) bool {
	switch policy {
	case enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL:
		return true
	case enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER:
		// CHASM terminates the paused workflow and continues; V1 stalls (bug).
		return isCHASM
	default:
		// SKIP, BUFFER_ONE, BUFFER_ALL, CANCEL_OTHER: the paused workflow keeps
		// the slot, so the schedule does not take new actions.
		return false
	}
}

func TestScheduleV1PauseInteraction(t *testing.T) {
	runSchedulePauseOverlapMatrix(t, v1ContextFactory, false /* isCHASM */)
}

func TestScheduleCHASMPauseInteraction(t *testing.T) {
	runSchedulePauseOverlapMatrix(t, chasmContextFactory, true /* isCHASM */)
}

func runSchedulePauseOverlapMatrix(t *testing.T, newContext contextFactory, isCHASM bool) {
	for _, policy := range allOverlapPolicies {
		policy := policy
		t.Run(policy.String(), func(t *testing.T) {
			testSchedulePauseOverlap(t, newContext, isCHASM, policy)
		})
	}
}

// pauseInteractionOpts returns the schedule test options plus the dynamic config
// required to enable the workflow pause feature.
func pauseInteractionOpts(t *testing.T) []testcore.TestOption {
	return append(scheduleCommonOpts(t),
		testcore.WithDynamicConfig(dynamicconfig.WorkflowPauseEnabled, true),
	)
}

// testSchedulePauseOverlap creates a schedule (1s interval) that starts a single
// long-running workflow, pauses that workflow, and then asserts whether the
// schedule keeps taking scheduled actions, per expectScheduleProgressesWhilePaused.
func testSchedulePauseOverlap(t *testing.T, newContext contextFactory, isCHASM bool, policy enumspb.ScheduleOverlapPolicy) {
	s := testcore.NewEnv(t, pauseInteractionOpts(t)...)

	sid := testcore.RandomizeStr("sched-pause-" + policy.String())
	wid := testcore.RandomizeStr("sched-pause-wf-" + policy.String())
	wt := testcore.RandomizeStr("sched-pause-wt-" + policy.String())

	// A workflow that runs (effectively) forever, so it stays in the
	// schedule's running set until the schedule terminates it.
	s.SdkWorker().RegisterWorkflowWithOptions(
		func(ctx workflow.Context) error {
			return workflow.Sleep(ctx, time.Hour)
		},
		workflow.RegisterOptions{Name: wt},
	)

	schedule := &schedulepb.Schedule{
		Spec: &schedulepb.ScheduleSpec{
			Interval: []*schedulepb.IntervalSpec{
				{Interval: durationpb.New(1 * time.Second)},
			},
		},
		Action: &schedulepb.ScheduleAction{
			Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   wid,
					WorkflowType: &commonpb.WorkflowType{Name: wt},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: s.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
				},
			},
		},
		Policies: &schedulepb.SchedulePolicies{
			OverlapPolicy: policy,
		},
	}

	ctx := newContext(s.Context())
	_, err := s.FrontendClient().CreateSchedule(ctx, &workflowservice.CreateScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
		Schedule:   schedule,
		Identity:   "test",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Wait for the schedule to start its first workflow. RecentActions is
	// populated for every overlap policy (including ALLOW_ALL) and by both
	// schedulers, unlike RunningWorkflows which V1 does not populate for
	// ALLOW_ALL.
	var execution *commonpb.WorkflowExecution
	await.RequireTruef(t, func() bool {
		desc, descErr := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
			Namespace:  s.Namespace().String(),
			ScheduleId: sid,
		})
		if descErr != nil {
			return false
		}
		for _, ra := range desc.GetInfo().GetRecentActions() {
			if ex := ra.GetStartWorkflowResult(); ex.GetRunId() != "" {
				execution = ex
				return true
			}
		}
		return false
	}, 30*time.Second, 250*time.Millisecond, "schedule should start its first workflow")

	// Pause that workflow.
	_, err = s.FrontendClient().PauseWorkflowExecution(ctx, &workflowservice.PauseWorkflowExecutionRequest{
		Namespace:  s.Namespace().String(),
		WorkflowId: execution.GetWorkflowId(),
		RunId:      execution.GetRunId(),
		Identity:   "functional-test",
		Reason:     "schedule-pause-overlap-interaction",
		RequestId:  uuid.NewString(),
	})
	require.NoError(t, err)

	// Confirm the workflow reaches PAUSED.
	await.RequireTrue(t, func() bool {
		d, dErr := s.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.Namespace().String(),
			Execution: execution,
		})
		return dErr == nil && d.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_PAUSED
	}, 15*time.Second, 200*time.Millisecond)

	actionsAtPause, err := scheduleActionCount(ctx, s, sid)
	require.NoError(t, err)
	fmt.Printf("[DEBUGGING] [chasm=%t %s] paused wf=%s actionsAtPause=%d\n",
		isCHASM, policy, execution.GetRunId(), actionsAtPause)

	if expectScheduleProgressesWhilePaused(isCHASM, policy) {
		// The schedule should keep taking new actions despite the paused workflow.
		await.RequireTruef(t, func() bool {
			c, cErr := scheduleActionCount(ctx, s, sid)
			return cErr == nil && c > actionsAtPause
		}, 20*time.Second, 500*time.Millisecond,
			"schedule with %s should keep taking actions while its workflow is paused", policy)
	} else {
		// The schedule must not take any new action while the workflow is
		// paused: the paused workflow keeps the overlap slot occupied. There is
		// no await helper for asserting the *absence* of progress, so wait a
		// fixed window during which the 1s schedule would otherwise fire ~8
		// times, then assert ActionCount did not advance.
		//nolint:forbidigo // asserting absence of scheduled actions over a fixed window
		time.Sleep(8 * time.Second)
		got, gErr := scheduleActionCount(ctx, s, sid)
		require.NoError(t, gErr)
		require.Equal(t, actionsAtPause, got,
			"schedule with %s must not take new actions while its workflow is paused", policy)
	}
}

// scheduleActionCount returns the schedule's total ActionCount (number of
// workflows it has started).
func scheduleActionCount(ctx context.Context, s *testcore.TestEnv, sid string) (int64, error) {
	desc, err := s.FrontendClient().DescribeSchedule(ctx, &workflowservice.DescribeScheduleRequest{
		Namespace:  s.Namespace().String(),
		ScheduleId: sid,
	})
	if err != nil {
		return 0, err
	}
	return desc.GetInfo().GetActionCount(), nil
}
