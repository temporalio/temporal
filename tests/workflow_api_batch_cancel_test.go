package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchCancelClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchCancelClientTestSuite]
}

func TestWorkflowAPIBatchCancelClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchCancelClientTestSuite{})
}

// cancelableWorkflow blocks until its context is canceled, then returns the
// resulting cancellation error so the workflow reaches the Canceled status.
func cancelableWorkflow(ctx workflow.Context) error {
	ctx.Done().Receive(ctx, nil)
	return ctx.Err()
}

func (s *WorkflowAPIBatchCancelClientTestSuite) TestWorkflowBatchCancel_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchCancelClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := env.Context()

			workflowType := testcore.RandomizeStr(t.Name())
			env.SdkWorker().RegisterWorkflowWithOptions(cancelableWorkflow, workflow.RegisterOptions{Name: workflowType})

			// Start three workflows of the same (per-test, unique) type.
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

			// Cancel all three workflows with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_CancellationOperation{
					CancellationOperation: &batchpb.BatchOperationCancellation{
						Identity: "batch-canceler",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_CANCEL_WORKFLOW, expectedQuery, expectedExecutions)

			// All three workflows must reach the Canceled status.
			for _, e := range executions {
				//nolint:forbidigo // for tests with waits
				require.Eventually(t, func() bool {
					desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: env.Namespace().String(),
						Execution: e,
					})
					return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED
				}, 5*time.Second, 100*time.Millisecond)
			}
		})
	}
}
