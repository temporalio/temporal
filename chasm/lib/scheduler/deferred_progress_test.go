package scheduler_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Buffer processing runs inline when the engine commits EnqueueBufferedStarts.
// A discarded selection must release its followers without an external wakeup.
func TestDeferredStartsAfterDiscard(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			for _, reason := range []string{"catchup", "multiple expired", "paused", "exhausted"} {
				t.Run(reason, func(t *testing.T) {
					t.Parallel()
					ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
					env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
					var invoker *scheduler.Invoker
					require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
						now := ts.Now()
						selectedTime := now
						if reason == "catchup" || reason == "multiple expired" {
							selectedTime = now.Add(-2 * defaultCatchupWindow)
						}
						s.Schedule.State.Paused = reason == "paused"
						s.Schedule.State.LimitedActions = reason == "exhausted"
						invoker = s.Invoker.Get(ctx)
						starts := []*schedulespb.BufferedStart{progressStart("selected", selectedTime, policy)}
						if reason == "multiple expired" {
							// BUFFER_ONE retains one follower; BUFFER_ALL retains the whole queue.
							if policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL {
								starts = append(starts, progressStart("also-expired", selectedTime, policy))
							}
						}
						next := progressStart("next", now, policy)
						next.Manual = reason == "paused" || reason == "exhausted"
						starts = append(starts, next)
						invoker.EnqueueBufferedStarts(ctx, starts)
						return nil
					}))
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						starts := s.Invoker.Get(ctx).GetBufferedStarts()
						require.Len(t, starts, 1)
						require.Equal(t, "next", starts[0].GetRequestId())
						require.Equal(t, int64(1), starts[0].GetAttempt(), "discarded selection must not strand its follower")
						missed := int64(0)
						if reason == "catchup" || reason == "multiple expired" {
							missed = 1
						}
						if reason == "multiple expired" && policy == enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL {
							missed = 2
						}
						require.Equal(t, missed, s.Info.GetMissedCatchupWindow())
						return nil
					}))
					require.Positive(t, progressExecuteTaskCount(t, env), "discard recovery must emit a physical execute task")
					client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
					client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Cond(func(req *workflowservice.StartWorkflowExecutionRequest) bool {
						return req.RequestId == "next"
					})).Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "next-run"}, nil)
					executeProgressTask(t, env, invoker, client)
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						require.Equal(t, int64(1), s.Info.GetActionCount())
						require.Equal(t, "next-run", s.Invoker.Get(ctx).GetBufferedStarts()[0].GetRunId())
						return nil
					}))
				})
			}
		})
	}
}

// Run actual start attempts, including the backoff timers, before checking that
// a terminal failure releases the next start in the same engine transaction.
func TestDeferredStartsAfterStartFailure(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			for _, failure := range []struct {
				name     string
				err      error
				attempts int
			}{
				{"already started", serviceerror.NewWorkflowExecutionAlreadyStarted("already started", "", "other-run"), 1},
				{"invalid request", serviceerror.NewInvalidArgument("invalid request"), 1},
				{"retry exhaustion", serviceerror.NewUnavailable("unavailable"), scheduler.InvokerMaxStartAttempts},
			} {
				t.Run(failure.name, func(t *testing.T) {
					t.Parallel()
					ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
					schedule := defaultSchedule()
					// Manual actions still run while paused; automatic generator ticks cannot
					// introduce unrelated starts as the retry clock advances.
					schedule.State.Paused = true
					env := newSchedulerTestEngine(t, schedule, withEngineTimeSource(ts))
					var invoker *scheduler.Invoker
					require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
						selected := progressStart("selected", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
						selected.Manual = true
						invoker = s.Invoker.Get(ctx)
						invoker.EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{selected})
						return nil
					}))

					require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
						next := progressStart("next", ts.Now(), policy)
						next.Manual = true
						s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{next})
						return nil
					}))
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						starts := s.Invoker.Get(ctx).GetBufferedStarts()
						require.Len(t, starts, 2)
						require.Equal(t, int64(-1), starts[1].GetAttempt())
						return nil
					}))
					client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
					client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Cond(func(req *workflowservice.StartWorkflowExecutionRequest) bool {
						return req.RequestId == "selected"
					})).Return(nil, failure.err).Times(failure.attempts)
					var beforeTerminal int
					for attempt := 1; attempt <= failure.attempts; attempt++ {
						beforeTerminal = progressExecuteTaskCount(t, env)
						executeProgressTask(t, env, invoker, client)
						if failure.attempts == 1 {
							break
						}
						var backoff time.Time
						require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
							starts := s.Invoker.Get(ctx).GetBufferedStarts()
							require.Len(t, starts, 2)
							require.Equal(t, int64(attempt+1), starts[0].GetAttempt())
							require.Equal(t, int64(-1), starts[1].GetAttempt(), "retrying start must keep its follower deferred")
							backoff = starts[0].GetBackoffTime().AsTime()
							require.True(t, backoff.After(ts.Now()))
							return nil
						}))
						ts.Update(backoff)
						_, err := env.engine.FirePureTasks(env.rootRef, backoff)
						require.NoError(t, err)
					}
					if failure.attempts > 1 {
						// The attempt beyond the limit is rejected locally, without another RPC.
						beforeTerminal = progressExecuteTaskCount(t, env)
						executeProgressTask(t, env, invoker, client)
					}
					require.Greater(t, progressExecuteTaskCount(t, env), beforeTerminal, "terminal failure must emit a new physical execute task")
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						starts := s.Invoker.Get(ctx).GetBufferedStarts()
						require.Len(t, starts, 1)
						require.Equal(t, "next", starts[0].GetRequestId())
						require.Equal(t, int64(1), starts[0].GetAttempt(), "terminal start failure must not strand its follower")
						require.Zero(t, s.Info.GetActionCount())
						return nil
					}))
					client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Cond(func(req *workflowservice.StartWorkflowExecutionRequest) bool {
						return req.RequestId == "next"
					})).Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "next-run"}, nil)
					executeProgressTask(t, env, invoker, client)
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						require.Equal(t, int64(1), s.Info.GetActionCount())
						require.Equal(t, "next-run", s.Invoker.Get(ctx).GetBufferedStarts()[0].GetRunId())
						return nil
					}))
				})
			}
		})
	}
}

func TestDeferredStartsReconsideredWithoutBlocker(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			for _, blocker := range []string{"none", "completed", "running", "selected", "backoff"} {
				t.Run(blocker, func(t *testing.T) {
					t.Parallel()
					ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
					env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
					require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
						invoker := s.Invoker.Get(ctx)
						if blocker != "none" {
							start := progressStart("blocker", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
							start.Attempt = 1
							if blocker == "running" || blocker == "completed" {
								start.RunId = "blocker-run"
								start.StartTime = timestamppb.New(ts.Now())
								start.HasCallback = true
							}
							if blocker == "completed" {
								start.Completed = &schedulespb.CompletedResult{Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, CloseTime: timestamppb.New(ts.Now())}
							}
							if blocker == "backoff" {
								start.BackoffTime = timestamppb.New(ts.Now().Add(time.Second))
							}
							invoker.BufferedStarts = append(invoker.BufferedStarts, start)
						}
						deferred := progressStart("deferred", ts.Now(), policy)
						deferred.Attempt = -1
						invoker.BufferedStarts = append(invoker.BufferedStarts, deferred)
						// Model a previously stranded entry encountering the common rearming path.
						invoker.EnqueueBufferedStarts(ctx, nil)
						return nil
					}))
					require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
						starts := s.Invoker.Get(ctx).GetBufferedStarts()
						follower := starts[len(starts)-1]
						require.Equal(t, "deferred", follower.GetRequestId())
						wantAttempt := int64(-1)
						if blocker == "none" || blocker == "completed" {
							wantAttempt = 1
						}
						require.Equal(t, wantAttempt, follower.GetAttempt(), "blocker=%s", blocker)
						return nil
					}))
				})
			}
		})
	}
}

func progressStart(id string, at time.Time, policy enumspb.ScheduleOverlapPolicy) *schedulespb.BufferedStart {
	return &schedulespb.BufferedStart{
		RequestId: id, WorkflowId: id, NominalTime: timestamppb.New(at), ActualTime: timestamppb.New(at), DesiredTime: timestamppb.New(at), OverlapPolicy: policy,
	}
}

func executeProgressTask(t *testing.T, env *schedulerTestEngine, invoker *scheduler.Invoker, client *workflowservicemock.MockWorkflowServiceClient) {
	t.Helper()
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config: defaultConfig(), MetricsHandler: metrics.NoopMetricsHandler, BaseLogger: env.logger, FrontendClient: client,
	})
	dropped, err := chasmtest.ExecuteSideEffectTask(context.Background(), env.engine, invoker, handler, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, dropped, "a workflow-start task must be armed and eligible")
}

// Pending start requests must enforce overlap policy before their RunId is
// recorded. Once recorded, cancel/terminate followers must target that run.
func TestDeferredStartsWaitForPendingStart(t *testing.T) {
	t.Parallel()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
			env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
			var invoker *scheduler.Invoker
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				invoker = s.Invoker.Get(ctx)
				invoker.EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{progressStart("selected", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)})
				return nil
			}))
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{progressStart("next", ts.Now(), policy)})
				return nil
			}))
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				starts := s.Invoker.Get(ctx).GetBufferedStarts()
				require.Len(t, starts, 2)
				require.Equal(t, int64(1), starts[0].GetAttempt())
				require.Equal(t, int64(-1), starts[1].GetAttempt(), "pending start must block overlapping work before its RunId is recorded")
				return nil
			}))
			client := workflowservicemock.NewMockWorkflowServiceClient(gomock.NewController(t))
			client.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Cond(func(req *workflowservice.StartWorkflowExecutionRequest) bool {
				return req.RequestId == "selected"
			})).Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "selected-run"}, nil)
			executeProgressTask(t, env, invoker, client)
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				i := s.Invoker.Get(ctx)
				require.Len(t, i.GetBufferedStarts(), 2)
				require.Equal(t, "selected-run", i.GetBufferedStarts()[0].GetRunId())
				require.Equal(t, int64(-1), i.GetBufferedStarts()[1].GetAttempt())
				wantCancels, wantTerminates := 0, 0
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER {
					wantCancels = 1
				}
				if policy == enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER {
					wantTerminates = 1
				}
				require.Len(t, i.GetCancelWorkflows(), wantCancels)
				require.Len(t, i.GetTerminateWorkflows(), wantTerminates)
				if wantCancels > 0 {
					require.Equal(t, "selected-run", i.GetCancelWorkflows()[0].GetRunId())
				}
				if wantTerminates > 0 {
					require.Equal(t, "selected-run", i.GetTerminateWorkflows()[0].GetRunId())
				}
				return nil
			}))
		})
	}
}

func TestDeferredStartsDoNotWaitForAllowAll(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{
			progressStart("expired", ts.Now().Add(-2*defaultCatchupWindow), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL),
			progressStart("overlapping", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL),
			progressStart("next", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL),
		})
		return nil
	}))
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		starts := s.Invoker.Get(ctx).GetBufferedStarts()
		require.Len(t, starts, 2)
		require.Equal(t, "overlapping", starts[0].GetRequestId())
		require.Equal(t, int64(1), starts[0].GetAttempt())
		require.Equal(t, "next", starts[1].GetRequestId())
		require.Equal(t, int64(1), starts[1].GetAttempt(), "ALLOW_ALL must not become a completion dependency")
		return nil
	}))
}

func TestDeferredStartsPreserveOrderWhenRearmed(t *testing.T) {
	t.Parallel()
	ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
	require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
		invoker := s.Invoker.Get(ctx)
		old := progressStart("old", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
		old.Attempt = -1
		invoker.BufferedStarts = append(invoker.BufferedStarts, old)
		invoker.EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{
			progressStart("new", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL),
		})
		return nil
	}))
	require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
		starts := s.Invoker.Get(ctx).GetBufferedStarts()
		require.Len(t, starts, 2)
		require.Equal(t, "old", starts[0].GetRequestId())
		require.Equal(t, int64(1), starts[0].GetAttempt())
		require.Equal(t, "new", starts[1].GetRequestId())
		require.Equal(t, int64(-1), starts[1].GetAttempt())
		return nil
	}))
}

func TestPendingStartSkipAndAllowAll(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		policy  enumspb.ScheduleOverlapPolicy
		starts  int
		skipped int64
	}{
		{enumspb.SCHEDULE_OVERLAP_POLICY_SKIP, 1, 1},
		{enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, 2, 0},
	} {
		t.Run(tc.policy.String(), func(t *testing.T) {
			t.Parallel()
			ts := clock.NewEventTimeSource().Update(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
			env := newSchedulerTestEngine(t, defaultSchedule(), withEngineTimeSource(ts))
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{
					progressStart("selected", ts.Now(), enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL),
				})
				return nil
			}))
			require.NoError(t, env.updateScheduler(func(s *scheduler.Scheduler, ctx chasm.MutableContext) error {
				s.Invoker.Get(ctx).EnqueueBufferedStarts(ctx, []*schedulespb.BufferedStart{progressStart("next", ts.Now(), tc.policy)})
				return nil
			}))
			require.NoError(t, env.readScheduler(func(s *scheduler.Scheduler, ctx chasm.Context) error {
				starts := s.Invoker.Get(ctx).GetBufferedStarts()
				require.Len(t, starts, tc.starts)
				require.Equal(t, tc.skipped, s.Info.GetOverlapSkipped())
				for _, start := range starts {
					require.Equal(t, int64(1), start.GetAttempt())
				}
				return nil
			}))
		})
	}
}

func progressExecuteTaskCount(t *testing.T, env *schedulerTestEngine) int {
	t.Helper()
	pending, err := env.engine.Tasks(env.rootRef)
	require.NoError(t, err)
	count := 0
	for _, task := range pending[tasks.CategoryTransfer] {
		if task, ok := task.(*tasks.ChasmTask); ok && slices.Equal(task.Info.GetPath(), []string{"Invoker"}) {
			count++
		}
	}
	return count
}
