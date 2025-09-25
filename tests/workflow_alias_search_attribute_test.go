package tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAliasSearchAttributeTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowAliasSearchAttributeTestSuite(t *testing.T) {
	s := new(WorkflowAliasSearchAttributeTestSuite)
	suite.Run(t, s)
}

func (s *WorkflowAliasSearchAttributeTestSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
}

func (w *WorkflowAliasSearchAttributeTestSuite) WorkflowFunc(ctx workflow.Context) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    10 * time.Second,
		ScheduleToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    1 * time.Second,
			BackoffCoefficient: 1,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    3,
		},
	}), w.ActivityFunc).Get(ctx, nil)
	return err
}

func (w *WorkflowAliasSearchAttributeTestSuite) ActivityFunc() (string, error) {
	return "done!", nil
}

func (s *WorkflowAliasSearchAttributeTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction, prefix string, scheduleById string) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(prefix + "-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	if scheduleById != "" {
		workflowOptions.SearchAttributes = map[string]interface{}{
			"ScheduleById": scheduleById,
		}
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.WorkflowFunc)
	s.Worker().RegisterActivity(s.ActivityFunc)

	workflowRun1 := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_1", "")
	workflowRun2 := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_2", "")

	// Wait for workflows to complete
	err := workflowRun1.Get(ctx, nil)
	s.NoError(err)
	err = workflowRun2.Get(ctx, nil)
	s.NoError(err)

	getWFResponse, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
	s.NoError(err)
	s.NotNil(getWFResponse)
	s.Equal(workflowRun1.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	s.Equal(workflowRun1.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	getWFResponse, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
	s.NoError(err)
	s.NotNil(getWFResponse)
	s.Equal(workflowRun2.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	s.Equal(workflowRun2.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	// Use Eventually pattern to wait for visibility store to be updated
	s.Eventually(
		func() bool {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowId = '" + workflowRun1.GetID() + "' OR WorkflowId = '" + workflowRun2.GetID() + "'",
			})
			s.NoError(err)
			s.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	s.Eventually(
		func() bool {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "TemporalScheduledById IS NULL",
			})
			s.NoError(err)
			s.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	s.Eventually(
		func() bool {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "ScheduledById IS NULL",
			})
			s.NoError(err)
			s.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (s *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute_WithClash() {
	ctx, cancel := context.WithTimeout(context.Background(), 30000*time.Second)
	defer cancel()

	s.Worker().RegisterWorkflow(s.WorkflowFunc)
	s.Worker().RegisterActivity(s.ActivityFunc)

	// Add `ScheduleById = Randy` search attribute to the workflow
	_, err := s.SdkClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: s.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"ScheduleById": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	s.NoError(err)

	workflowRun1 := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_1", "Randy")
	workflowRun2 := s.createWorkflow(ctx, s.WorkflowFunc, "wf_id_2", "Randy")

	// Wait for workflows to complete
	err = workflowRun1.Get(ctx, nil)
	s.NoError(err)
	err = workflowRun2.Get(ctx, nil)
	s.NoError(err)

	getWFResponse, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
	s.NoError(err)
	s.NotNil(getWFResponse)
	s.Equal(workflowRun1.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	s.Equal(workflowRun1.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	getWFResponse, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
	s.NoError(err)
	s.NotNil(getWFResponse)
	s.Equal(workflowRun2.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	s.Equal(workflowRun2.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	// Use Eventually pattern to wait for visibility store to be updated
	s.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "WorkflowId = '" + workflowRun1.GetID() + "' OR WorkflowId = '" + workflowRun2.GetID() + "'",
			})
			require.NoError(c, err)
			require.NotNil(c, resp)
			require.Equal(c, len(resp.GetExecutions()), 2)
		},
		30*time.Second,
		250*time.Millisecond,
	)

	s.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "TemporalScheduledById IS NULL",
			})
			require.NoError(c, err)
			require.NotNil(c, resp)
			require.Equal(c, len(resp.GetExecutions()), 0)
		},
		30*time.Second,
		250*time.Millisecond,
	)

	s.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := s.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				Query:     "ScheduledById IS NOT NULL",
			})
			require.NoError(c, err)
			require.NotNil(c, resp)
			require.Equal(c, len(resp.GetExecutions()), 2)
		},
		30*time.Second,
		250*time.Millisecond,
	)
}
