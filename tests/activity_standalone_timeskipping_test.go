package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/testing/testcontext"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestStandaloneActivityTimeSkipping(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name       string
		startDelay time.Duration
		retry      bool
	}{
		{name: "StartDelay", startDelay: time.Hour},
		{name: "RetryBackoff", retry: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			env := testcore.NewEnv(t,
				testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
				testcore.WithDynamicConfig(dynamicconfig.StandaloneActivityTimeSkippingEnabled, true),
			)
			ctx := testcontext.For(t)
			tv := env.Tv()
			client := env.FrontendClient()

			start, err := client.StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
				Namespace:              env.Namespace().String(),
				ActivityId:             tv.ActivityID(),
				ActivityType:           tv.ActivityType(),
				TaskQueue:              tv.TaskQueue(),
				RequestId:              tv.RequestID(),
				StartDelay:             durationpb.New(test.startDelay),
				StartToCloseTimeout:    durationpb.New(2 * time.Hour),
				ScheduleToStartTimeout: durationpb.New(time.Minute),
				ScheduleToCloseTimeout: durationpb.New(90 * time.Minute),
				RetryPolicy: &commonpb.RetryPolicy{
					InitialInterval: durationpb.New(time.Hour),
					MaximumAttempts: 2,
				},
			})
			require.NoError(t, err)

			readSkippedDuration := func() time.Duration {
				state, err := env.GetTestCluster().ExecutionManager().GetWorkflowExecution(ctx, &persistence.GetWorkflowExecutionRequest{
					ShardID: common.WorkflowIDToHistoryShard(
						env.NamespaceID().String(), tv.ActivityID(), env.GetTestClusterConfig().HistoryConfig.NumHistoryShards),
					NamespaceID: env.NamespaceID().String(),
					WorkflowID:  tv.ActivityID(),
					RunID:       start.GetRunId(),
					ArchetypeID: activity.ArchetypeID,
				})
				require.NoError(t, err)
				return state.State.ExecutionInfo.GetTimeSkippingInfo().GetAccumulatedSkippedDuration().AsDuration()
			}

			if test.startDelay > 0 {
				require.Greater(t, readSkippedDuration(), 30*time.Minute)
			} else {
				require.Zero(t, readSkippedDuration())
			}

			task, err := client.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
				Namespace: env.Namespace().String(),
				TaskQueue: tv.TaskQueue(),
				Identity:  tv.WorkerIdentity(),
			})
			require.NoError(t, err)
			require.NotEmpty(t, task.GetTaskToken())
			require.EqualValues(t, 1, task.GetAttempt())

			if test.retry {
				_, err := client.RespondActivityTaskFailed(ctx, &workflowservice.RespondActivityTaskFailedRequest{
					Namespace: env.Namespace().String(),
					TaskToken: task.GetTaskToken(),
					Failure: &failurepb.Failure{
						Message: "retry once",
						FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
							ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{Type: "retryable"},
						},
					},
				})
				require.NoError(t, err)
				require.Greater(t, readSkippedDuration(), 30*time.Minute)

				task, err = client.PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
					Namespace: env.Namespace().String(),
					TaskQueue: tv.TaskQueue(),
					Identity:  tv.WorkerIdentity(),
				})
				require.NoError(t, err)
				require.NotEmpty(t, task.GetTaskToken())
				require.EqualValues(t, 2, task.GetAttempt())
			}

			described, err := client.DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: tv.ActivityID(),
				RunId:      start.GetRunId(),
			})
			require.NoError(t, err)
			skipped := readSkippedDuration()
			// SDK workers calculate their deadline from these task fields using the wall clock.
			require.Equal(t, described.GetInfo().GetExpirationTime().AsTime().Add(-skipped),
				task.GetScheduledTime().AsTime().Add(task.GetScheduleToCloseTimeout().AsDuration()))
			require.Equal(t, described.GetInfo().GetLastStartedTime().AsTime().Add(-skipped), task.GetStartedTime().AsTime())

			result := payloads.EncodeString("done")
			_, err = client.RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
				Namespace: env.Namespace().String(),
				TaskToken: task.GetTaskToken(),
				Result:    result,
			})
			require.NoError(t, err)

			outcome, err := client.PollActivityExecution(ctx, &workflowservice.PollActivityExecutionRequest{
				Namespace:  env.Namespace().String(),
				ActivityId: tv.ActivityID(),
				RunId:      start.GetRunId(),
			})
			require.NoError(t, err)
			protorequire.ProtoEqual(t, result, outcome.GetOutcome().GetResult())
		})
	}
}
