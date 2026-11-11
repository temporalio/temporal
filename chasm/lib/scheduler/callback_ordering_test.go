package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestIdleOrderingFinalStartUpdateAndLegacyCallback(t *testing.T) {
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		for _, order := range []string{"update_callback_before_record", "callback_update_before_record", "update_before_record_callback_after", "record_callback_update"} {
			t.Run(policy.String()+"/"+order, func(t *testing.T) {
				base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
				scheduled := base.Add(2 * scheduler.DefaultTweakables.IdleTime)
				updateTime := scheduled.Add(time.Minute)
				ts := clock.NewEventTimeSource()
				ts.Update(base)
				schedule := defaultSchedule()
				schedule.Spec = &schedulepb.ScheduleSpec{}
				schedule.Policies.OverlapPolicy = policy
				e := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts))
				ts.Update(scheduled)
				require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
					invoker := s.Invoker.Get(ctx)
					invoker.LastProcessedTime = timestamppb.New(scheduled)
					invoker.BufferedStarts = []*schedulespb.BufferedStart{{
						RequestId: "ordering-request", WorkflowId: "ordering-workflow", Attempt: 1, OverlapPolicy: policy,
						NominalTime: timestamppb.New(scheduled), ActualTime: timestamppb.New(scheduled), DesiredTime: timestamppb.New(scheduled),
					}}
					s.Generator.Get(ctx).Generate(ctx)
					return nil
				}))
				require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
					require.False(t, s.Closed)
					require.Equal(t, scheduled.Add(scheduler.DefaultTweakables.IdleTime), s.IdleCloseTime.AsTime(), "pending final action must establish its own retention interval")
					return nil
				}))
				update := func() {
					t.Helper()
					ts.Update(updateTime)
					replacement := defaultSchedule()
					replacement.Spec = &schedulepb.ScheduleSpec{}
					replacement.State.Notes = "updated while final start is in flight"
					// The buffered action keeps its original policy even if the schedule changes.
					replacement.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
					_, err := scheduler.NewTestHandler(e.logger).UpdateSchedule(e.engineCtx, &schedulerpb.UpdateScheduleRequest{
						NamespaceId: namespaceID, FrontendRequest: &workflowservice.UpdateScheduleRequest{Namespace: namespace, ScheduleId: scheduleID, Schedule: replacement},
					})
					require.NoError(t, err)
				}
				complete := func() {
					t.Helper()
					completion := &persistencespb.ChasmNexusCompletion{
						RequestId: "ordering-request", CloseTime: timestamppb.New(scheduled.Add(30 * time.Second)),
						Outcome: &persistencespb.ChasmNexusCompletion_Success{Success: &commonpb.Payload{Data: []byte("ordering-result")}},
					}
					for range 2 {
						require.NoError(t, e.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
							return s.HandleNexusCompletion(ctx, completion)
						}))
					}
				}
				frontend := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
				frontend.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
					DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
						require.Equal(t, "ordering-request", request.GetRequestId())
						require.Equal(t, "ordering-workflow", request.GetWorkflowId())
						if policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
							require.Empty(t, request.GetCompletionCallbacks())
							// Model a deduplicated retry of a start accepted with a callback before rollout.
						} else {
							require.Len(t, request.GetCompletionCallbacks(), 1)
						}
						switch order {
						case "update_callback_before_record":
							update()
							complete()
						case "callback_update_before_record":
							complete()
							update()
						case "update_before_record_callback_after":
							update()
						case "record_callback_update":
						default:
							t.Fatalf("unknown ordering %q", order)
						}
						return &workflowservice.StartWorkflowExecutionResponse{RunId: "ordering-run"}, nil
					})
				handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
					Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: e.logger, FrontendClient: frontend,
				})
				require.NoError(t, handler.Execute(e.engineCtx, callbackOrderingInvokerRef(t, e), chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{}))
				if order == "update_before_record_callback_after" {
					complete()
				}
				if order == "record_callback_update" {
					complete()
					update()
				}
				var deadline time.Time
				require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
					require.False(t, s.Closed)
					require.Equal(t, int64(1), s.Info.ActionCount, "accepted external start must be counted exactly once despite early/duplicate callbacks")
					require.Equal(t, updateTime, s.Info.UpdateTime.AsTime())
					var recorded time.Time
					if policy == enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL {
						require.Empty(t, s.Invoker.Get(ctx).BufferedStarts)
						require.Len(t, s.Info.RecentActions, 1)
						action := s.Info.RecentActions[0]
						require.Equal(t, "ordering-run", action.GetStartWorkflowResult().GetRunId())
						recorded = action.GetActualTime().AsTime()
					} else {
						starts := s.Invoker.Get(ctx).BufferedStarts
						require.Len(t, starts, 1)
						require.Equal(t, "ordering-request", starts[0].RequestId)
						require.Equal(t, "ordering-run", starts[0].RunId)
						require.Equal(t, policy, starts[0].OverlapPolicy)
						require.NotNil(t, starts[0].Completed)
						require.Equal(t, []byte("ordering-result"), s.LastCompletionResult.Get(ctx).Success.GetData())
						recorded = starts[0].StartTime.AsTime()
					}
					require.False(t, recorded.IsZero())
					// StartWorkflow stamps wall time; the ordering driver observes that input.
					latest := updateTime
					if recorded.After(latest) {
						latest = recorded
					}
					deadline = latest.Add(scheduler.DefaultTweakables.IdleTime)
					require.Equal(t, deadline, s.IdleCloseTime.AsTime())
					return nil
				}))
				ts.Update(deadline.Truncate(common.ScheduledTaskMinPrecision).Add(-time.Nanosecond))
				_, err := e.engine.FirePureTasks(e.rootRef, ts.Now())
				require.NoError(t, err)
				require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error { require.False(t, s.Closed); return nil }))
				ts.Update(deadline)
				_, err = e.engine.FirePureTasks(e.rootRef, deadline)
				require.NoError(t, err)
				require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, _ chasm.Context) error {
					require.True(t, s.Closed)
					require.Equal(t, int64(1), s.Info.ActionCount)
					return nil
				}))
			})
		}
	}
}

func callbackOrderingInvokerRef(t *testing.T, e *schedulerTestEngine) chasm.ComponentRef {
	t.Helper()
	var encoded []byte
	require.NoError(t, e.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		var err error
		encoded, err = ctx.Ref(s.Invoker.Get(ctx))
		return err
	}))
	ref, err := chasm.DeserializeComponentRef(encoded)
	require.NoError(t, err)
	return ref
}
