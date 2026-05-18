package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
)

type ActivityReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	activityShouldFail atomic.Bool
}

func TestActivityReportedProblemsTestSuite(t *testing.T) {
	s := new(ActivityReportedProblemsTestSuite)
	suite.Run(t, s)
}

func (s *ActivityReportedProblemsTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	// Trigger after 3 retry attempts (i.e., activity is on its 3rd or higher attempt).
	s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveActivityRetryProblemsToTriggerSearchAttribute, 3)
}

func (s *ActivityReportedProblemsTestSuite) failingActivity() (string, error) {
	if s.activityShouldFail.Load() {
		return "", temporal.NewApplicationError("forced-activity-failure", "ForcedError")
	}
	return "done!", nil
}

// workflowWithRetryingActivity runs an activity with a retry policy and waits for it to finish.
func (s *ActivityReportedProblemsTestSuite) workflowWithRetryingActivity(ctx workflow.Context) (string, error) {
	var ret string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 10,
		},
	}), s.failingActivity).Get(ctx, &ret)
	if err != nil {
		return "", err
	}
	return ret, nil
}

// TestActivityReportedProblems_SetAndClear verifies that the TemporalReportedProblems search
// attribute is set once the activity retry threshold is reached, and cleared when the activity
// eventually succeeds.
func (s *ActivityReportedProblemsTestSuite) TestActivityReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.activityShouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivity)
	s.SdkWorker().RegisterActivity(s.failingActivity)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivity)
	s.NoError(err)

	// Wait until TemporalReportedProblems is set with the activity retry category.
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.Contains(t, saVal, "category=ActivityRetryFailed")
	}, 20*time.Second, 500*time.Millisecond)

	// Unblock the activity so the workflow can complete.
	s.activityShouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))
	s.Equal("done!", out)

	// After the workflow completes, the search attribute should be gone.
	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

// TestActivityReportedProblems_DisabledByDefault verifies that when the threshold is set to 0
// (the default), the search attribute is never set even when the activity retries many times.
func (s *ActivityReportedProblemsTestSuite) TestActivityReportedProblems_DisabledByDefault() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Override to disabled (threshold = 0).
	cleanup := s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveActivityRetryProblemsToTriggerSearchAttribute, 0)
	defer cleanup()

	s.activityShouldFail.Store(true)

	s.SdkWorker().RegisterWorkflow(s.workflowWithRetryingActivity)
	s.SdkWorker().RegisterActivity(s.failingActivity)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithRetryingActivity)
	s.NoError(err)

	// Wait for several retries but confirm the SA is never set.
	s.EventuallyWithT(func(t *assert.CollectT) {
		exec, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		// Confirm at least 3 retry attempts have happened.
		require.NotEmpty(t, exec.PendingActivities)
		require.GreaterOrEqual(t, exec.PendingActivities[0].Attempt, int32(3))
	}, 20*time.Second, 500*time.Millisecond)

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok, "TemporalReportedProblems should not be set when threshold is disabled")

	// Clean up.
	s.NoError(s.SdkClient().TerminateWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID(), "test cleanup"))
}
