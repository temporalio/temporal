package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// executeOrderState is the observable outcome of one selected start.
type executeOrderState struct {
	ActionCount      int64
	RecentActions    []string // run ID and status
	RunningWorkflows int
	BufferedStarts   []string // request ID, run ID, and completion status
	LastSuccess      string
}

// TestExecuteResult_CompletionOrderIndependent verifies that a start's
// completion callback produces the same scheduler state whether it arrives
// while the start RPC is in flight or after the Execute task records the
// result.
func TestExecuteResult_CompletionOrderIndependent(t *testing.T) {
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		for _, firstAttemptLost := range []bool{false, true} {
			name := policy.String()
			if firstAttemptLost {
				name += "/first response lost"
			}
			t.Run(name, func(t *testing.T) {
				during := runExecuteOrder(t, policy, firstAttemptLost, true)
				after := runExecuteOrder(t, policy, firstAttemptLost, false)
				require.Equal(t, after, during)
				require.Equal(t, int64(1), after.ActionCount)
			})
		}
	}
}

// runExecuteOrder runs one selected start through the Execute task. When
// firstAttemptLost is set, the first start RPC is accepted but returns a
// retryable error, and a second Execute task records the run.
func runExecuteOrder(
	t *testing.T,
	policy enumspb.ScheduleOverlapPolicy,
	firstAttemptLost bool,
	completeDuringRPC bool,
) executeOrderState {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	ts := clock.NewEventTimeSource()
	ts.Update(now)
	schedule := defaultSchedule()
	schedule.Spec = &schedulepb.ScheduleSpec{}
	schedule.Policies.OverlapPolicy = policy
	e := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts))
	require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		invoker := s.Invoker.Get(ctx)
		invoker.LastProcessedTime = timestamppb.New(now)
		invoker.BufferedStarts = []*schedulespb.BufferedStart{{
			RequestId: "req", WorkflowId: "wf", Attempt: 1, OverlapPolicy: policy,
			NominalTime: timestamppb.New(now), ActualTime: timestamppb.New(now),
		}}
		return nil
	}))

	complete := func() {
		require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			return s.HandleNexusCompletion(ctx, &persistencespb.ChasmNexusCompletion{
				RequestId: "req",
				CloseTime: timestamppb.New(now.Add(time.Second)),
				Outcome:   &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{Data: []byte("result")}},
			})
		}))
	}

	calls := 0
	frontend := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
	frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes().
		DoAndReturn(func(context.Context, *workflowservice.StartWorkflowExecutionRequest, ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			calls++
			if calls == 1 && completeDuringRPC {
				complete()
			}
			if calls == 1 && firstAttemptLost {
				return nil, serviceerror.NewUnavailable("response lost")
			}
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run"}, nil
		})
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: e.logger, FrontendClient: frontend,
	})
	execute := func() {
		require.NoError(t, handler.Execute(e.engineCtx, callbackOrderingInvokerRef(t, e), chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{}))
	}

	execute()
	if !completeDuringRPC {
		complete()
	}
	if firstAttemptLost {
		// Advance past the retry backoff so the start is eligible again.
		require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
			invoker := s.Invoker.Get(ctx)
			for _, start := range invoker.BufferedStarts {
				invoker.LastProcessedTime = start.BackoffTime
			}
			return nil
		}))
		execute()
	}

	var state executeOrderState
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		describe, err := s.Describe(ctx, &schedulerpb.DescribeScheduleRequest{}, newLegacySpecBuilder(0, 0))
		if err != nil {
			return err
		}
		info := describe.GetFrontendResponse().GetInfo()
		state.ActionCount = info.GetActionCount()
		state.RunningWorkflows = len(info.GetRunningWorkflows())
		for _, action := range info.GetRecentActions() {
			state.RecentActions = append(state.RecentActions,
				action.GetStartWorkflowResult().GetRunId()+" "+action.GetStartWorkflowStatus().String())
		}
		for _, start := range s.Invoker.Get(ctx).BufferedStarts {
			state.BufferedStarts = append(state.BufferedStarts,
				start.GetRequestId()+" "+start.GetRunId()+" "+start.GetCompleted().GetStatus().String())
		}
		state.LastSuccess = string(s.LastCompletionResult.Get(ctx).GetSuccess().GetData())
		return nil
	}))
	return state
}
