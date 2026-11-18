package tests

import (
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	chasmscheduler "go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (suite *ScheduleCHASMSuite) TestStartsAfterAPSRetryExhaustion() {
	t := suite.T()
	for _, policy := range []enumspb.ScheduleOverlapPolicy{
		enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	} {
		t.Run(policy.String(), func(t *testing.T) {
			t.Parallel()
			opts := append(scheduleCommonOpts(t),
				testcore.WithDynamicConfig(chasmscheduler.RetryPolicyInitialInterval, 10*time.Millisecond),
				testcore.WithDynamicConfig(chasmscheduler.RetryPolicyMaximumInterval, 10*time.Millisecond),
			)
			env := newScheduleEnv(t, opts...)
			ctx := chasmContextFactory(testcore.NewContext())
			capture := env.StartNamespaceMetricCapture()
			logs := env.StartNamespaceLogCapture()
			sid, wid, wt := testcore.RandomizeStr("aps-recovery"), env.Tv().WorkflowID(), env.Tv().WorkflowType().Name
			var runs, rejected atomic.Int32
			var firstRequestID atomic.Value
			firstRequestID.Store("")
			registerCountingWorkflow(env, wt, &runs)
			env.InjectRequestFault(func(req any) error {
				if request, ok := req.(*workflowservice.StartWorkflowExecutionRequest); ok {
					firstRequestID.CompareAndSwap("", request.RequestId)
					if request.RequestId != firstRequestID.Load().(string) {
						return nil
					}
					rejected.Add(1)
					return &serviceerror.ResourceExhausted{Cause: enumspb.RESOURCE_EXHAUSTED_CAUSE_APS_LIMIT, Message: "APS limit"}
				}
				return nil
			})
			first := time.Now().UTC().Truncate(time.Second).Add(3 * time.Second)
			second := first.Add(10 * time.Second)
			sched := &schedulepb.Schedule{
				Spec:     &schedulepb.ScheduleSpec{Calendar: []*schedulepb.CalendarSpec{calendarSpec(first), calendarSpec(second)}},
				Action:   startWorkflowAction(env, wid, wt),
				Policies: &schedulepb.SchedulePolicies{OverlapPolicy: policy, CatchupWindow: durationpb.New(time.Hour)},
			}
			createSchedule(ctx, t, env, sid, sched)
			tags := map[string]string{metrics.ScheduleBackendTag: metrics.ScheduleBackendChasm, metrics.ScheduleActionTypeTag: metrics.ScheduleActionStartWorkflow}
			await.Require(ctx, t, func(t *await.T) {
				// Side-effect tasks can be delivered more than once, so their
				// attempt metrics can exceed the logical retry budget.
				require.GreaterOrEqual(t, countMetric(capture, metrics.ScheduleActionErrors.Name(), tags), chasmscheduler.InvokerMaxStartAttempts+1)
				require.True(t, slices.ContainsFunc(logs.Snapshot(), func(record testlogger.CapturedLog) bool {
					if record.Level != testlogger.Info || record.Message != "failed to start workflow" {
						return false
					}
					for _, field := range record.Tags {
						if field.Key() == "error" && fmt.Sprint(field.Value()) == "unprocessable task: retry limit exceeded" {
							return true
						}
					}
					return false
				}), "expected the terminal retry-exhaustion log")
			}, 30*time.Second, pollInterval)
			require.GreaterOrEqual(t, rejected.Load(), int32(chasmscheduler.InvokerMaxStartAttempts))

			// The later occurrence must start without a patch/update waking the
			// schedule, while the original request remains permanently rejected.
			await.Require(ctx, t, func(t *await.T) {
				response, err := env.FrontendClient().DescribeSchedule(t.Context(), &workflowservice.DescribeScheduleRequest{
					Namespace: env.Namespace().String(), ScheduleId: sid,
				})
				require.NoError(t, err)
				require.Equal(t, int64(1), response.Info.ActionCount)
				require.Zero(t, response.Info.BufferSize)
				require.Len(t, response.Info.RecentActions, 1)
				require.Equal(t, second, response.Info.RecentActions[0].ScheduleTime.AsTime())
				require.NotEmpty(t, response.Info.RecentActions[0].StartWorkflowResult.RunId)
				require.Equal(t, int32(1), runs.Load())
				require.GreaterOrEqual(t, countMetric(capture, metrics.ScheduleActionSuccess.Name(), tags), 1)
			}, 30*time.Second, pollInterval)
		})
	}
}
