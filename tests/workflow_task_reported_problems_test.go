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

type WFTFailureReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	shouldFail   atomic.Bool
	failureCount atomic.Int32
	failureType  atomic.Int32 // 0 = panic, 1 = non-deterministic error
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

// workflowWithActivity creates a workflow that executes an activity before potentially failing.
// This is used to test workflow task failure scenarios in a more realistic context where the workflow
// has already executed some operations (activities) before encountering a workflow task failure.
// The activity itself succeeds, but the workflow task may fail afterward, which triggers the server
// to clear the sticky task queue and transition to a normal task queue for subsequent workflow tasks.
func (s *WFTFailureReportedProblemsTestSuite) workflowWithActivity(ctx workflow.Context) (string, error) {
	var ret string
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Second,
	}), s.simpleActivity).Get(ctx, &ret)
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
		saVal, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saVal)
		require.Contains(t, saVal, "category=WorkflowTaskFailed")
		require.Contains(t, saVal, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")

		execution, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, execution.PendingWorkflowTask.Attempt, int32(2))
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear_FailAfterActivity() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.shouldFail.Store(true)

	s.Worker().RegisterWorkflow(s.workflowWithActivity)
	s.Worker().RegisterActivity(s.simpleActivity)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, s.workflowWithActivity)
	s.NoError(err)

	// Validate the search attributes are not empty and has TemporalReportedProblems with 2 entries
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_DynamicConfigChanges() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cleanup := s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 0)
	defer cleanup()
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
		_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.False(t, ok)

		exec, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.GreaterOrEqual(t, exec.PendingWorkflowTask.Attempt, int32(2))
	}, 10*time.Second, 500*time.Millisecond)

	cleanup2 := s.OverrideDynamicConfig(dynamicconfig.NumConsecutiveWorkflowTaskProblemsToTriggerSearchAttribute, 2)
	defer cleanup2()

	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		saValues, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
		require.True(t, ok)
		require.NotEmpty(t, saValues)
		require.Len(t, saValues, 2)
		require.Contains(t, saValues, "category=WorkflowTaskFailed")
		require.Contains(t, saValues, "cause=WorkflowTaskFailedCauseWorkflowWorkerUnhandledFailure")
	}, 15*time.Second, 500*time.Millisecond)

	s.shouldFail.Store(false)

	var out string
	s.NoError(workflowRun.Get(ctx, &out))

	description, err := s.SdkClient().DescribeWorkflow(ctx, workflowRun.GetID(), workflowRun.GetRunID())
	s.NoError(err)
	s.NotNil(description.TypedSearchAttributes)
	_, ok := description.TypedSearchAttributes.GetKeywordList(temporal.NewSearchAttributeKeyKeywordList(sadefs.TemporalReportedProblems))
	s.False(ok)
}
