package tests

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchDeleteClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchDeleteClientTestSuite]
}

func TestWorkflowAPIBatchDeleteClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchDeleteClientTestSuite{})
}

func (s *WorkflowAPIBatchDeleteClientTestSuite) TestWorkflowBatchDelete_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchDeleteClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := s.Context()

			workflowType := testcore.RandomizeStr(t.Name())
			env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

			// Start three workflows of the same (per-test, unique) type. Batch
			// delete works on running or closed workflows, so no need to close
			// them first.
			executions := make([]*commonpb.WorkflowExecution, 0, 3)
			for i := range 3 {
				run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:        testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i)),
					TaskQueue: env.WorkerTaskQueue(),
				}, workflowType)
				s.NoError(err)
				executions = append(executions, &commonpb.WorkflowExecution{
					WorkflowId: run.GetID(),
					RunId:      run.GetRunID(),
				})
			}

			// Delete all three workflows with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_DeletionOperation{
					DeletionOperation: &batchpb.BatchOperationDeletion{
						Identity: "batch-deleter",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_DELETE_WORKFLOW, expectedQuery, expectedExecutions)

			// All three workflows must be deleted (no longer describable).
			for _, e := range executions {
				//nolint:forbidigo // for tests with waits
				require.Eventually(t, func() bool {
					_, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: env.Namespace().String(),
						Execution: e,
					})
					var notFoundErr *serviceerror.NotFound
					return errors.As(err, &notFoundErr)
				}, 5*time.Second, 100*time.Millisecond)
			}
		})
	}
}
