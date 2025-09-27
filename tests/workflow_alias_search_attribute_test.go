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

func (ts *WorkflowAliasSearchAttributeTestSuite) SetupTest() {
	ts.FunctionalTestBase.SetupTest()
}

func (ts *WorkflowAliasSearchAttributeTestSuite) WorkflowFunc(ctx workflow.Context) error {
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
	}), ts.ActivityFunc).Get(ctx, nil)
	return err
}

func (ts *WorkflowAliasSearchAttributeTestSuite) ActivityFunc() (string, error) {
	return "done!", nil
}

func (ts *WorkflowAliasSearchAttributeTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction, prefix string, scheduleByID string) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(prefix + "-" + ts.T().Name()),
		TaskQueue: ts.TaskQueue(),
	}
	if scheduleByID != "" {
		scheduleByIDKey := temporal.NewSearchAttributeKeyKeyword("CustomerSA")
		workflowOptions.TypedSearchAttributes = temporal.NewSearchAttributes(
			scheduleByIDKey.ValueSet(scheduleByID),
		)
	}
	workflowRun, err := ts.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	ts.NoError(err)
	ts.NotNil(workflowRun)

	return workflowRun
}

func (ts *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts.Worker().RegisterWorkflow(ts.WorkflowFunc)
	ts.Worker().RegisterActivity(ts.ActivityFunc)

	workflowRun1 := ts.createWorkflow(ctx, ts.WorkflowFunc, "wf_id_1", "")
	workflowRun2 := ts.createWorkflow(ctx, ts.WorkflowFunc, "wf_id_2", "")

	// Wait for workflows to complete
	err := workflowRun1.Get(ctx, nil)
	ts.NoError(err)
	err = workflowRun2.Get(ctx, nil)
	ts.NoError(err)

	getWFResponse, err := ts.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
	ts.NoError(err)
	ts.NotNil(getWFResponse)
	ts.Equal(workflowRun1.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	ts.Equal(workflowRun1.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	getWFResponse, err = ts.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
	ts.NoError(err)
	ts.NotNil(getWFResponse)
	ts.Equal(workflowRun2.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	ts.Equal(workflowRun2.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	// Use Eventually pattern to wait for visibility store to be updated
	ts.Eventually(
		func() bool {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
				Query:     "WorkflowId = '" + workflowRun1.GetID() + "' OR WorkflowId = '" + workflowRun2.GetID() + "'",
			})
			ts.NoError(err)
			ts.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	ts.Eventually(
		func() bool {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
				Query:     "TemporalScheduledById IS NULL",
			})
			ts.NoError(err)
			ts.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)

	ts.Eventually(
		func() bool {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
				Query:     "ScheduledById IS NULL",
			})
			ts.NoError(err)
			ts.NotNil(resp)
			return len(resp.GetExecutions()) == 2
		},
		testcore.WaitForESToSettle,
		100*time.Millisecond,
	)
}

func (ts *WorkflowAliasSearchAttributeTestSuite) TestWorkflowAliasSearchAttribute_WithCustomSA() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ts.Worker().RegisterWorkflow(ts.WorkflowFunc)
	ts.Worker().RegisterActivity(ts.ActivityFunc)

	// Add `ScheduleById = Randy` search attribute to the workflow
	_, err := ts.SdkClient().OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace: ts.Namespace().String(),
		SearchAttributes: map[string]enumspb.IndexedValueType{
			"CustomerSA": enumspb.INDEXED_VALUE_TYPE_KEYWORD,
		},
	})
	ts.NoError(err)

	workflowRun1 := ts.createWorkflow(ctx, ts.WorkflowFunc, "wf_id_1", "Randy")
	workflowRun2 := ts.createWorkflow(ctx, ts.WorkflowFunc, "wf_id_2", "Randy")

	// Wait for workflows to complete
	err = workflowRun1.Get(ctx, nil)
	ts.NoError(err)
	err = workflowRun2.Get(ctx, nil)
	ts.NoError(err)

	getWFResponse, err := ts.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
	ts.NoError(err)
	ts.NotNil(getWFResponse)
	ts.Equal(workflowRun1.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	ts.Equal(workflowRun1.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	getWFResponse, err = ts.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
	ts.NoError(err)
	ts.NotNil(getWFResponse)
	ts.Equal(workflowRun2.GetID(), getWFResponse.GetWorkflowExecutionInfo().Execution.WorkflowId)
	ts.Equal(workflowRun2.GetRunID(), getWFResponse.GetWorkflowExecutionInfo().Execution.RunId)

	// Use Eventually pattern to wait for visibility store to be updated
	ts.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
				Query:     "WorkflowId = '" + workflowRun1.GetID() + "' OR WorkflowId = '" + workflowRun2.GetID() + "'",
			})
			require.NoError(c, err)
			require.NotNil(c, resp)
			require.Equal(c, len(resp.GetExecutions()), 2)
		},
		30*time.Second,
		250*time.Millisecond,
	)

	ts.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
				Query:     "TemporalScheduledById IS NULL",
			})
			require.NoError(c, err)
			require.NotNil(c, resp)
			require.Equal(c, len(resp.GetExecutions()), 0)
		},
		30*time.Second,
		250*time.Millisecond,
	)

	ts.EventuallyWithT(
		func(c *assert.CollectT) {
			resp, err := ts.SdkClient().ListWorkflow(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: ts.Namespace().String(),
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
