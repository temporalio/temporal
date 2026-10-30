package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/payloads"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestCatchupLoopDebug runs the first V1 scheduler workflow task after a stall
// of `gap`, for an every-second schedule with a long catchup window, so the
// loop in processTimeRange can be stepped through. Edit the constants to change
// the scenario.
//
//	TEMPORAL_DEBUG=1 dlv test ./service/worker/scheduler -- -test.run '^TestCatchupLoopDebug$' -test.v
//	(dlv) break loop workflow.go:842                 # addStart call in processTimeRange's loop
//	(dlv) condition -hitcount loop % 3600            # stop once per hour of schedule time
//	(dlv) break full workflow.go:1521                # addStart's "Buffer too large" path
//	(dlv) condition full len(s.State.BufferedStarts) == 1000
//	(dlv) continue
//	(dlv) print next.Next
//
// Conditions can't call methods (next.Next.Second() fails to evaluate, so dlv
// stops every time); use hit counts or plain fields. TEMPORAL_DEBUG=1 turns off
// the SDK deadlock detector and test timeout, which a breakpoint would trip.
func TestCatchupLoopDebug(t *testing.T) {
	const (
		gap           = 24 * time.Hour
		catchupWindow = 365 * 24 * time.Hour
		overlap       = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	)

	// Run a single iteration, then continue-as-new.
	prev := CurrentTweakablePolicies.IterationsBeforeContinueAsNew
	CurrentTweakablePolicies.IterationsBeforeContinueAsNew = 1
	t.Cleanup(func() { CurrentTweakablePolicies.IterationsBeforeContinueAsNew = prev })

	now := time.Date(2026, 10, 16, 0, 0, 0, 0, time.UTC)
	everySecond := []*schedulepb.Range{{Start: 0, End: 59}}
	args := &schedulespb.StartScheduleArgs{
		Schedule: &schedulepb.Schedule{
			Spec: &schedulepb.ScheduleSpec{
				StructuredCalendar: []*schedulepb.StructuredCalendarSpec{{
					Second:     everySecond,
					Minute:     everySecond,
					Hour:       []*schedulepb.Range{{Start: 0, End: 23}},
					DayOfMonth: []*schedulepb.Range{{Start: 1, End: 31}},
					Month:      []*schedulepb.Range{{Start: 1, End: 12}},
					DayOfWeek:  []*schedulepb.Range{{Start: 0, End: 6}},
				}},
				TimezoneName: "Etc/GMT",
			},
			Action: &schedulepb.ScheduleAction{Action: &schedulepb.ScheduleAction_StartWorkflow{
				StartWorkflow: &workflowpb.NewWorkflowExecutionInfo{
					WorkflowId:   "myid",
					WorkflowType: &commonpb.WorkflowType{Name: "mywf"},
					TaskQueue:    &taskqueuepb.TaskQueue{Name: "mytq"},
				},
			}},
			Policies: &schedulepb.SchedulePolicies{
				OverlapPolicy: overlap,
				CatchupWindow: durationpb.New(catchupWindow),
			},
		},
		Info: &schedulepb.ScheduleInfo{},
		State: &schedulespb.InternalState{
			Namespace:         "myns",
			NamespaceId:       "mynsid",
			ScheduleId:        "myschedule",
			ConflictToken:     InitialConflictToken,
			LastProcessedTime: timestamppb.New(now.Add(-gap)),
		},
	}

	var ts testsuite.WorkflowTestSuite
	// The default test logger prints two lines per walked match.
	ts.SetLogger(log.NewSdkLogger(log.NewNoopLogger()))
	env := ts.NewTestWorkflowEnvironment()
	env.SetStartTime(now)
	var started []string
	env.OnActivity(new(activities).StartWorkflow, mock.Anything, mock.Anything).Return(
		func(_ context.Context, req *schedulespb.StartWorkflowRequest) (*schedulespb.StartWorkflowResponse, error) {
			started = append(started, req.Request.WorkflowId)
			return &schedulespb.StartWorkflowResponse{RunId: "run", RealStartTime: timestamppb.New(env.Now())}, nil
		})
	env.OnActivity(new(activities).WatchWorkflow, mock.Anything, mock.Anything).Maybe().Return(
		&schedulespb.WatchWorkflowResponse{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED}, nil)

	env.ExecuteWorkflow(func(ctx workflow.Context) error {
		return SchedulerWorkflow(ctx, args)
	})

	require.True(t, env.IsWorkflowCompleted())
	var canErr *workflow.ContinueAsNewError
	require.ErrorAs(t, env.GetWorkflowError(), &canErr)
	var next *schedulespb.StartScheduleArgs
	require.NoError(t, payloads.Decode(canErr.Input, &next))

	// Every second of the gap was walked; the buffer keeps the first 1000 and
	// SKIP starts only the earliest of those.
	items := int64(gap / time.Second)
	t.Logf("walked %d matches: bufferDropped=%d overlapSkipped=%d started=%v lastProcessedTime=%s",
		items, next.Info.BufferDropped, next.Info.OverlapSkipped, started, next.State.LastProcessedTime.AsTime())
	require.Equal(t, items-1000, next.Info.BufferDropped)
	require.Equal(t, []string{"myid-" + now.Add(-gap+time.Second).Format(time.RFC3339)}, started)
	require.Equal(t, now, next.State.LastProcessedTime.AsTime())
}
