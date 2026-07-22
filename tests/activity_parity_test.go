package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	activitypb "go.temporal.io/api/activity/v1"
	commonpb "go.temporal.io/api/common/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// activityParityTestSuite verifies that workflow activities (WFA) and
// standalone activities (SAA) behave the same way when a caller replaces the
// complete retry policy via the activity-options update APIs.
type activityParityTestSuite struct {
	parallelsuite.Suite[*activityParityTestSuite]
}

func TestActivityParityTestSuite(t *testing.T) {
	parallelsuite.Run(t, &activityParityTestSuite{})
}

// TestWFA_RetryPolicyUpdate_PersistsNonRetryableErrorTypes replaces the
// complete retry policy on a pending workflow activity with one that
// includes NonRetryableErrorTypes, and verifies that the value survives
// into a subsequent DescribeWorkflowExecution call, not just the update
// response.
func (s *activityParityTestSuite) TestWFA_RetryPolicyUpdate_PersistsNonRetryableErrorTypes() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	env := testcore.NewEnv(t)

	const activityID = "activity-id"
	var startedActivityCount atomic.Int32
	activityFn := func() (string, error) {
		startedActivityCount.Add(1)
		return "", errors.New("bad-luck-please-retry")
	}

	workflowFn := func(ctx workflow.Context) error {
		var ret string
		return workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             activityID,
			DisableEagerExecution:  true,
			ScheduleToCloseTimeout: 30 * time.Minute,
			StartToCloseTimeout:    30 * time.Minute,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    10 * time.Minute,
				BackoffCoefficient: 1,
				MaximumAttempts:    100,
			},
		}), activityFn).Get(ctx, &ret)
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)
	env.SdkWorker().RegisterActivity(activityFn)

	workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        "activity-parity-wfa-" + testcore.RandomizeStr(t.Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	// Wait for the activity to fail once and enter its (long) retry backoff,
	// so it's pending when we update its retry policy.
	await.Require(ctx, t, func(t *await.T) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Equal(t, int32(1), startedActivityCount.Load())
	}, 10*time.Second, 200*time.Millisecond)

	nonRetryableType := "my-non-retryable-error"
	updateResp, err := env.FrontendClient().UpdateActivityOptions(ctx, &workflowservice.UpdateActivityOptionsRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowRun.GetID()},
		Activity:  &workflowservice.UpdateActivityOptionsRequest_Id{Id: activityID},
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:        durationpb.New(1 * time.Second),
				BackoffCoefficient:     2,
				MaximumInterval:        durationpb.New(10 * time.Second),
				MaximumAttempts:        100,
				NonRetryableErrorTypes: []string{nonRetryableType},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
	})
	s.NoError(err)
	s.Equal([]string{nonRetryableType}, updateResp.GetActivityOptions().GetRetryPolicy().GetNonRetryableErrorTypes(),
		"update response should echo the requested non-retryable error types")

	description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.Len(description.GetPendingActivities(), 1)
	s.Equal([]string{nonRetryableType}, description.PendingActivities[0].GetActivityOptions().GetRetryPolicy().GetNonRetryableErrorTypes(),
		"Describe should retain the non-retryable error types that were just persisted via the retry policy update")
}

// TestSAA_RetryPolicyUpdate_PersistsNonRetryableErrorTypes runs the same
// scenario as the WFA test above, but against a standalone activity.
func (s *activityParityTestSuite) TestSAA_RetryPolicyUpdate_PersistsNonRetryableErrorTypes() {
	t := s.T()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	env := testcore.NewEnv(t)
	defer env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)()
	defer env.OverrideDynamicConfig(activity.Enabled, true)()

	activityID := testcore.RandomizeStr(t.Name())
	taskQueue := testcore.RandomizeStr(t.Name())

	startResp, err := env.FrontendClient().StartActivityExecution(ctx, &workflowservice.StartActivityExecutionRequest{
		Namespace:           env.Namespace().String(),
		ActivityId:          activityID,
		ActivityType:        &commonpb.ActivityType{Name: "test-activity"},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue},
		StartToCloseTimeout: durationpb.New(30 * time.Minute),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval: durationpb.New(10 * time.Minute),
			MaximumAttempts: 100,
		},
	})
	s.NoError(err)

	nonRetryableType := "my-non-retryable-error"
	updateResp, err := env.FrontendClient().UpdateActivityExecutionOptions(ctx, &workflowservice.UpdateActivityExecutionOptionsRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
		ActivityOptions: &activitypb.ActivityOptions{
			RetryPolicy: &commonpb.RetryPolicy{
				InitialInterval:        durationpb.New(1 * time.Second),
				BackoffCoefficient:     2,
				MaximumInterval:        durationpb.New(10 * time.Second),
				MaximumAttempts:        100,
				NonRetryableErrorTypes: []string{nonRetryableType},
			},
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"retry_policy"}},
	})
	s.NoError(err)
	s.Equal([]string{nonRetryableType}, updateResp.GetActivityOptions().GetRetryPolicy().GetNonRetryableErrorTypes(),
		"update response should echo the requested non-retryable error types")

	descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: activityID,
		RunId:      startResp.RunId,
	})
	s.NoError(err)
	s.Equal([]string{nonRetryableType}, descResp.GetInfo().GetRetryPolicy().GetNonRetryableErrorTypes(),
		"standalone activity Describe should retain the non-retryable error types that were just persisted via the retry policy update")
}
