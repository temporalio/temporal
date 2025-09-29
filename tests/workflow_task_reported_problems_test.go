package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
)

type WFTFailureReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	shouldFail atomic.Bool
}

func TestWFTFailureReportedProblemsTestSuite(t *testing.T) {
	s := new(WFTFailureReportedProblemsTestSuite)
	suite.Run(t, s)
}

func (s *WFTFailureReportedProblemsTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
}

func (s *WFTFailureReportedProblemsTestSuite) simpleWorkflowWithShouldFail(ctx workflow.Context) (string, error) {
	if s.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}
	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) simpleActivity() (string, error) {
	return "done!", nil
}

// workflowWithActivity is used to trigger the transition from a sticky task queue to a non-sticky task queue
func (s *WFTFailureReportedProblemsTestSuite) workflowWithActivity(ctx workflow.Context) (string, error) {
	var ret string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{}), s.simpleActivity).Get(ctx, &ret)
	if err != nil {
		return "", err
	}

	if s.shouldFail.Load() {
		panic("forced-panic-to-fail-wft")
	}

	return "done!", nil
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.Worker().RegisterWorkflow(s.simpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflowWithShouldFail)
	s.NoError(err)

	// Check if the search attributes are not empty and has TemporalReportedProblems
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.TypedSearchAttributes)
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(searchattribute.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowWorkerUnhandledFailure")
	}, 5*time.Second, 500*time.Millisecond)

	s.EventuallyWithT(func(t *assert.CollectT) {
		queriedWorkflows, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "TemporalReportedProblems IN ('category=WorkflowTaskFailed', 'cause=WorkflowWorkerUnhandledFailure')",
			PageSize:  100,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(queriedWorkflows.Executions))
		require.Equal(t, workflowRun.GetID(), queriedWorkflows.Executions[0].Execution.WorkflowId)
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	// Validate the search attribute has been cleared
	s.EventuallyWithT(func(t *assert.CollectT) {
		queriedWorkflows, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "TemporalReportedProblems IS NULL",
			PageSize:  100,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(queriedWorkflows.Executions))
		require.Equal(t, workflowRun.GetID(), queriedWorkflows.Executions[0].Execution.WorkflowId)
	}, 5*time.Second, 500*time.Millisecond)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear_FailAfterActivity() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.Worker().RegisterWorkflow(s.simpleWorkflowWithShouldFail)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.simpleWorkflowWithShouldFail)
	s.NoError(err)

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.Status)
	}, 5*time.Second, 500*time.Millisecond)

	// Validate the search attributes are not empty and has TemporalReportedProblems with 2 entries
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.TypedSearchAttributes)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(searchattribute.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowWorkerUnhandledFailure")
	}, 5*time.Second, 500*time.Millisecond)

	// Check if the search attributes are queryable
	s.EventuallyWithT(func(t *assert.CollectT) {
		queriedWorkflows, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "TemporalReportedProblems IN ('category=WorkflowTaskFailed', 'cause=WorkflowWorkerUnhandledFailure')",
			PageSize:  100,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(queriedWorkflows.Executions))
		require.Equal(t, workflowRun.GetID(), queriedWorkflows.Executions[0].Execution.WorkflowId)
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	// Validate the search attribute has been cleared
	s.EventuallyWithT(func(t *assert.CollectT) {
		queriedWorkflows, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			Query:     "TemporalReportedProblems IS NULL",
			PageSize:  100,
		})
		require.NoError(t, err)
		require.Equal(t, 1, len(queriedWorkflows.Executions))
		require.Equal(t, workflowRun.GetID(), queriedWorkflows.Executions[0].Execution.WorkflowId)
	}, 5*time.Second, 500*time.Millisecond)
}
