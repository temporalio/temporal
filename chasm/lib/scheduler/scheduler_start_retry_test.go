package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestSchedulerStartsAfterAPSRetryExhaustion(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			sched := defaultSchedule()
			sched.Spec = &schedulepb.ScheduleSpec{Interval: []*schedulepb.IntervalSpec{{Interval: durationpb.New(time.Hour)}}}
			sched.Policies.OverlapPolicy = policy
			sched.Policies.CatchupWindow = durationpb.New(24 * time.Hour)
			env := newSchedulerTestEngine(t, sched)
			logger := env.logger.(*testlogger.TestLogger)
			logs := logger.StartCapture()
			defer logger.StopCapture(logs)
			recorder := metricstest.NewCaptureHandler()
			capture := recorder.StartCapture()
			defer recorder.StopCapture(capture)

			client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
			var requestIDs []string
			client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
				Times(scheduler.InvokerMaxStartAttempts + 1).
				DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
					requestIDs = append(requestIDs, req.RequestId)
					if len(requestIDs) <= scheduler.InvokerMaxStartAttempts {
						return nil, &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, Message: "APS limit"}
					}
					return &workflowservice.StartWorkflowExecutionResponse{RunId: "later-run"}, nil
				})
			handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
				Config:         defaultConfig(),
				MetricsHandler: recorder,
				BaseLogger:     env.logger,
				FrontendClient: client,
			})
			var invoker *scheduler.Invoker
			execute := func() {
				t.Helper()
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					invoker = s.Invoker.Get(ctx)
					return nil
				}))
				dropped, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, invoker, handler,
					chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
				require.NoError(t, err)
				require.False(t, dropped)
			}
			fire := func(at time.Time) {
				t.Helper()
				env.timeSource.Update(at)
				_, err := env.engine.FirePureTasks(env.rootRef, at)
				require.NoError(t, err)
			}

			first := env.timeSource.Now().UTC().Truncate(time.Hour).Add(time.Hour)
			fire(first)
			// Advance through persisted backoffs, including the final local
			// refusal that drops the start without another frontend RPC.
			for attempt := 1; attempt <= scheduler.InvokerMaxStartAttempts+1; attempt++ {
				execute()
				require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					starts := s.Invoker.Get(ctx).GetBufferedStarts()
					if attempt <= scheduler.InvokerMaxStartAttempts {
						require.Len(t, starts, 1)
						require.Equal(t, int64(attempt+1), starts[0].Attempt)
						require.True(t, starts[0].BackoffTime.AsTime().After(env.timeSource.Now()))
					} else {
						require.Empty(t, starts)
					}
					require.Zero(t, s.Info.ActionCount)
					require.Empty(t, s.Info.RecentActions)
					return nil
				}))
				if attempt <= scheduler.InvokerMaxStartAttempts {
					fire(invoker.BufferedStarts[0].BackoffTime.AsTime())
				}
			}
			logs.RequireContains(t, testlogger.CapturedLogPattern{
				Level: testlogger.Info, Message: "failed to start workflow",
				Tags: map[string]any{"error": "unprocessable task: retry limit exceeded", "wf-namespace": namespace, "schedule-id": scheduleID},
			})
			require.Len(t, requestIDs, scheduler.InvokerMaxStartAttempts)
			for _, id := range requestIDs {
				require.Equal(t, requestIDs[0], id, "retries must preserve the request ID")
			}

			second := first.Add(time.Hour)
			fire(second)
			execute()
			require.NotEqual(t, requestIDs[0], requestIDs[len(requestIDs)-1])
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				require.False(t, s.Closed)
				require.Equal(t, int64(1), s.Info.ActionCount)
				response, err := s.Describe(ctx, &schedulerpb.DescribeScheduleRequest{}, newLegacySpecBuilder(0, 0))
				require.NoError(t, err)
				actions := response.FrontendResponse.Info.RecentActions
				require.Len(t, actions, 1)
				require.Equal(t, second, actions[0].ScheduleTime.AsTime())
				require.Equal(t, "later-run", actions[0].StartWorkflowResult.RunId)
				return nil
			}))
			for name, count := range map[string]int{
				metrics.ScheduleActionErrors.Name():  scheduler.InvokerMaxStartAttempts + 1,
				metrics.ScheduleActionSuccess.Name(): 1,
			} {
				recordings := capture.Snapshot()[name]
				require.Len(t, recordings, count)
				for _, recording := range recordings {
					require.Equal(t, int64(1), recording.Value)
					require.Equal(t, namespace, recording.Tags["namespace"])
					require.Equal(t, "chasm", recording.Tags[metrics.ScheduleBackendTag])
					require.Equal(t, "start_workflow", recording.Tags[metrics.ScheduleActionTypeTag])
				}
			}
		})
	}
}
