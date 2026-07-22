package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type BatchOperationScaleTestSuite struct {
	parallelsuite.Suite[*BatchOperationScaleTestSuite]
}

func TestBatchOperationScaleTestSuite(t *testing.T) {
	parallelsuite.Run(t, &BatchOperationScaleTestSuite{})
}

// TestBatchTerminate_ManyWorkflows starts many workflows and terminates them all with a
// single batch operation, exercising the full StartBatchOperation -> batcher worker path
// (visibility paging, the worker pool, and per-task completion) end to end against a real
// server. It asserts every targeted workflow is processed.
func (s *BatchOperationScaleTestSuite) TestBatchTerminate_ManyWorkflows() {
	env := testcore.NewEnv(s.T(), testcore.WithWorkerService("batch operations"))
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	const numWorkflows = 20

	// Long-running workflow so the targets stay RUNNING until the batch terminates them.
	wfTypeName := testcore.RandomizeStr("batch-scale-wf")
	sleepWorkflow := func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, 24*time.Hour)
	}
	env.SdkWorker().RegisterWorkflowWithOptions(sleepWorkflow, workflow.RegisterOptions{Name: wfTypeName})

	runs := make([]sdkclient.WorkflowRun, 0, numWorkflows)
	for range numWorkflows {
		run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("batch-scale"),
			TaskQueue: env.WorkerTaskQueue(),
		}, wfTypeName)
		s.NoError(err)
		runs = append(runs, run)
	}

	query := fmt.Sprintf("WorkflowType='%s'", wfTypeName)

	// The batch targets workflows via a visibility query, so wait until all are indexed.
	s.EventuallyWithT(func(t *assert.CollectT) {
		resp, err := env.FrontendClient().CountWorkflowExecutions(ctx, &workflowservice.CountWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			Query:     query,
		})
		require.NoError(t, err)
		require.Equal(t, int64(numWorkflows), resp.GetCount())
	}, 15*time.Second, 500*time.Millisecond)

	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:       env.Namespace().String(),
		VisibilityQuery: query,
		JobId:           uuid.NewString(),
		Reason:          "batch-operation-scale-test",
		Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{},
		},
	})
	s.NoError(err)

	// Every workflow must reach TERMINATED, i.e. the batch processed all of them.
	s.EventuallyWithT(func(t *assert.CollectT) {
		for _, run := range runs {
			desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
			require.NoError(t, err)
			require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED, desc.WorkflowExecutionInfo.Status)
		}
	}, 30*time.Second, 500*time.Millisecond)
}
