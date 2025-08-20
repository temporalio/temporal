package tests

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowTaskReportedProblemsTestSuite struct {
	testcore.FunctionalTestBase
}

func TestWorkflowTaskReportedProblemsTestSuite(t *testing.T) {
	s := new(WorkflowTaskReportedProblemsTestSuite)
	suite.Run(t, s)
}

type panicWorkflow struct {
	allowContinue atomic.Bool
	startedCount  atomic.Int32
}

func newPanicWorkflow() *panicWorkflow {
	return &panicWorkflow{}
}

func (w *panicWorkflow) Workflow(ctx workflow.Context) error {
	w.startedCount.Add(1)
	if !w.allowContinue.Load() {
		panic("forced-panic-to-fail-wft")
	}
	// graceful completion
	return nil
}

func (s *WorkflowTaskReportedProblemsTestSuite) startWorkflow(ctx context.Context, wf any) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                  testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue:           s.TaskQueue(),
		WorkflowTaskTimeout: time.Second,
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, wf)
	s.NoError(err)
	s.NotNil(workflowRun)
	return workflowRun
}

func (s *WorkflowTaskReportedProblemsTestSuite) TestTemporalReportedProblems_SetAndClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pw := newPanicWorkflow()
	s.Worker().RegisterWorkflow(pw.Workflow)

	// Stop worker BEFORE starting workflow to force schedule->start WFT timeouts initially,
	// then start-to-close timeouts once a poll happens and handler panics.
	s.Worker().Stop()

	run := s.startWorkflow(ctx, pw.Workflow)

	// Verify TemporalReportedProblems search attribute is present in visibility after consecutive WFT timeouts/failures
	// It stores values like: ["property:reportedProblem=WorkflowTaskFailed" or "...TimedOut"]. We'll match TimedOut first.
	searchValue := "property:reportedProblem=WorkflowTaskTimedOut"
	escapedSearchValue := sqlparser.String(sqlparser.NewStrVal([]byte(searchValue)))
	query := fmt.Sprintf("(WorkflowId = '%s' AND %s = %s)", run.GetID(), searchattribute.TemporalReportedProblems, escapedSearchValue)

	var listResp *workflowservice.ListWorkflowExecutionsResponse
	s.EventuallyWithT(func(t *assert.CollectT) {
		var err error
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		if len(listResp.GetExecutions()) == 0 {
			// Fallback: try failure-based reported problem if timeout didn't trigger
			fallbackSearch := "property:reportedProblem=WorkflowTaskFailed"
			fallbackEscaped := sqlparser.String(sqlparser.NewStrVal([]byte(fallbackSearch)))
			fallbackQuery := fmt.Sprintf("(WorkflowId = '%s' AND %s = %s)", run.GetID(), searchattribute.TemporalReportedProblems, fallbackEscaped)
			listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
				Namespace: s.Namespace().String(),
				PageSize:  10,
				Query:     fallbackQuery,
			})
			require.NoError(t, err)
		}
		require.Len(t, listResp.GetExecutions(), 1)
		require.Equal(t, run.GetID(), listResp.GetExecutions()[0].GetExecution().GetWorkflowId())
	}, 60*time.Second, 250*time.Millisecond)

	// Flip the switch and restart worker to allow workflow to complete gracefully on next WFT
	pw.allowContinue.Store(true)
	s.NoError(s.Worker().Start())

	// Wait for completion
	var out any
	err := run.Get(ctx, &out)
	s.NoError(err)

	// Validate the search attribute was removed (no longer matches)
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp.GetExecutions(), 0)
	}, 20*time.Second, 250*time.Millisecond)
}
