package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/tests/testcore"
)

type ActivityAPIPausePolicyClientTestSuite struct {
	parallelsuite.Suite[*ActivityAPIPausePolicyClientTestSuite]
}

func TestActivityAPIPausePolicyClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIPausePolicyClientTestSuite{})
}

// TestActivityPausePolicy_AutoPauseThenContinue exercises the pause policy
// end-to-end through the SDK:
//  1. An activity declares a PausePolicy{MaxAttempts: 3} and keeps failing.
//  2. The server auto-pauses it once its just-failed attempt reaches the
//     threshold (the activity advances to attempt 4 and stops being dispatched).
//  3. After the activity is unpaused, it resumes and runs to completion. It
//     never auto-pauses again, even though its attempt count climbs well past
//     the policy threshold (the one-shot guarantee).
func (s *ActivityAPIPausePolicyClientTestSuite) TestActivityPausePolicy_AutoPauseThenContinue() {
	env := testcore.NewEnv(s.T(), testcore.WithSdkWorker())

	const pauseMaxAttempts = int32(3)
	scheduleToCloseTimeout := 30 * time.Minute
	startToCloseTimeout := 15 * time.Minute
	// Unlimited retries with a short, constant backoff so the activity reaches
	// the pause threshold quickly. Unlimited retries (MaximumAttempts: 0) ensures
	// the retry policy never supersedes the pause policy.
	activityRetryPolicy := &temporal.RetryPolicy{
		InitialInterval:    500 * time.Millisecond,
		BackoffCoefficient: 1,
	}

	makeWorkflowFunc := func(activityFunction ActivityFunctions) WorkflowFunction {
		return func(ctx workflow.Context) error {
			var ret string
			err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				ActivityID:             "activity-id",
				DisableEagerExecution:  true,
				StartToCloseTimeout:    startToCloseTimeout,
				ScheduleToCloseTimeout: scheduleToCloseTimeout,
				RetryPolicy:            activityRetryPolicy,
				PausePolicy:            temporal.PausePolicy{MaxAttempts: pauseMaxAttempts},
			}), activityFunction).Get(ctx, &ret)
			return err
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	var shouldSucceed atomic.Bool
	activityErr := errors.New("bad-luck-please-retry")

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if shouldSucceed.Load() {
			return "done!", nil
		}
		return "", activityErr
	}

	workflowFn := makeWorkflowFunc(activityFunction)

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// The activity should be auto-paused by its pause policy once the just-failed
	// attempt reaches pauseMaxAttempts. At that point the pending attempt is
	// pauseMaxAttempts+1, and the pause is attributed to the pause policy.
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, enumspb.PENDING_ACTIVITY_STATE_PAUSED, description.PendingActivities[0].State)
		require.Equal(t, pauseMaxAttempts+1, description.PendingActivities[0].Attempt)
		require.NotNil(t, description.PendingActivities[0].PauseInfo)
		require.NotNil(t, description.PendingActivities[0].PauseInfo.GetPausePolicy())
		require.Equal(t, pauseMaxAttempts, description.PendingActivities[0].PauseInfo.GetPausePolicy().GetMaxPauseDurationAttempts())
	}, 15*time.Second, 200*time.Millisecond)

	// While paused, the activity must not be dispatched again: the attempt count
	// stays put and the worker is not invoked further.
	pausedAtCount := startedActivityCount.Load()
	err = util.InterruptibleSleep(ctx, 2*time.Second)
	s.NoError(err)
	description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Len(description.PendingActivities, 1)
	s.True(description.PendingActivities[0].Paused)
	s.Equal(pauseMaxAttempts+1, description.PendingActivities[0].Attempt, "paused activity should not retry")
	s.Equal(pausedAtCount, startedActivityCount.Load(), "paused activity should not be dispatched to the worker")

	// Unpause the activity. It is still failing, so it should resume retrying.
	unpauseRequest := &workflowservice.UnpauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
		},
		Activity: &workflowservice.UnpauseActivityRequest_Id{Id: "activity-id"},
	}
	unpauseResp, err := env.FrontendClient().UnpauseActivity(ctx, unpauseRequest)
	s.NoError(err)
	s.NotNil(unpauseResp)

	// One-shot guarantee: after unpause the activity keeps retrying and its attempt
	// climbs past the pause threshold, but it must NOT auto-pause again.
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.False(t, description.PendingActivities[0].Paused, "activity must not re-pause after unpause")
		require.Greater(t, description.PendingActivities[0].Attempt, pauseMaxAttempts+1, "activity should keep retrying after unpause")
	}, 15*time.Second, 200*time.Millisecond)

	// Let the activity succeed and confirm the workflow runs to completion (the
	// workflow function returns only the activity's error, so success == nil error).
	shouldSucceed.Store(true)
	var out string
	err = workflowRun.Get(ctx, &out)
	s.NoError(err)
}
