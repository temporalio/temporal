package tests

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type WFTFailureReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration
	shouldFail             atomic.Bool

	activityRetryPolicy *temporal.RetryPolicy
}

func TestWFTFailureReportedProblemsTestSuite(t *testing.T) {
	s := new(WFTFailureReportedProblemsTestSuite)
	suite.Run(t, s)
}

func (s *WFTFailureReportedProblemsTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(s.Namespace())

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func (s *WFTFailureReportedProblemsTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) error {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    s.startToCloseTimeout,
			ScheduleToCloseTimeout: s.scheduleToCloseTimeout,
			RetryPolicy:            s.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)

		if !s.shouldFail.Load() {
			panic("forced-panic-to-fail-wft")
		}
		return err
	}
}

func (s *WFTFailureReportedProblemsTestSuite) TestWFTFailureReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityFunction := func() (string, error) {
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// Make sure the workflow has started and had an activity task scheduled and finished
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
	}, 5*time.Second, 500*time.Millisecond)

	s.EventuallyWithT(func(t *assert.CollectT) {
		wfHistory := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		})
		require.GreaterOrEqual(t, len(wfHistory), 1)
	}, 5*time.Second, 500*time.Millisecond)

	// Check if the search attributes are not empty and has TemporalReportedProblems
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes)
		require.NotEmpty(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])

		// Decode the search attribute in keyword list format
		searchValBytes := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems]
		searchVal, err := searchattribute.DecodeValue(searchValBytes, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
		require.NoError(t, err)
		require.NotEmpty(t, searchVal)
		require.Equal(t, "category=WorkflowTaskFailed", searchVal.([]string)[0])
		require.Equal(t, "cause=WorkflowWorkerUnhandledFailure", searchVal.([]string)[1])
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldFail.Store(true)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)

	// Validate the workflow completed successfully and the search attribute is removed
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.WorkflowExecutionInfo.Status)
		require.Nil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])
	}, 5*time.Second, 500*time.Millisecond)
}

type WFTTimedOutReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
	tv                        *testvars.TestVars
	initialRetryInterval      time.Duration
	scheduleToCloseTimeout    time.Duration
	startToCloseTimeout       time.Duration
	shouldStartToCloseTimeout atomic.Bool

	activityRetryPolicy *temporal.RetryPolicy
}

func TestWFTTimedOutReportedProblemsTestSuite(t *testing.T) {
	s := new(WFTTimedOutReportedProblemsTestSuite)
	suite.Run(t, s)
}

func (s *WFTTimedOutReportedProblemsTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(s.Namespace())

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 1 * time.Second

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func (s *WFTTimedOutReportedProblemsTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) error {
		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    s.startToCloseTimeout,
			ScheduleToCloseTimeout: s.scheduleToCloseTimeout,
			RetryPolicy:            s.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)

		if !s.shouldStartToCloseTimeout.Load() {
			time.Sleep(10 * time.Second)
		}
		return err
	}
}

func (s *WFTTimedOutReportedProblemsTestSuite) TestWFTTimedOutReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	activityFunction := func() (string, error) {
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}

	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)

	// Make sure the workflow has started and had an activity task scheduled and finished
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, description.WorkflowExecutionInfo.Status)
	}, 5*time.Second, 500*time.Millisecond)

	s.EventuallyWithT(func(t *assert.CollectT) {
		wfHistory := s.GetHistory(s.Namespace().String(), &commonpb.WorkflowExecution{
			WorkflowId: workflowRun.GetID(),
			RunId:      workflowRun.GetRunID(),
		})
		require.GreaterOrEqual(t, len(wfHistory), 1)
	}, 5*time.Second, 500*time.Millisecond)

	// Check if the search attributes are not empty and has TemporalReportedProblems
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes)
		require.NotEmpty(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields)
		require.NotNil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])

		// Decode the search attribute in keyword list format
		searchValBytes := description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems]
		searchVal, err := searchattribute.DecodeValue(searchValBytes, enumspb.INDEXED_VALUE_TYPE_KEYWORD_LIST, false)
		require.NoError(t, err)
		require.NotEmpty(t, searchVal)
		require.Equal(t, "category=WorkflowTaskFailed", searchVal.([]string)[0])
		require.Equal(t, "cause=WorkflowWorkerUnhandledFailure", searchVal.([]string)[1])
	}, 5*time.Second, 500*time.Millisecond)

	// Unblock the workflow
	s.shouldStartToCloseTimeout.Store(true)

	var out string
	err = workflowRun.Get(ctx, &out)

	s.NoError(err)

	// Validate the workflow completed successfully and the search attribute is removed
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		require.NoError(t, err)
		require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED, description.WorkflowExecutionInfo.Status)
		require.Nil(t, description.WorkflowExecutionInfo.SearchAttributes.IndexedFields[searchattribute.TemporalReportedProblems])
	}, 5*time.Second, 500*time.Millisecond)
}
