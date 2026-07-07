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
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type WorkflowAPIBatchUpdateOptionsClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchUpdateOptionsClientTestSuite]
}

func TestWorkflowAPIBatchUpdateOptionsClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchUpdateOptionsClientTestSuite{})
}

func (s *WorkflowAPIBatchUpdateOptionsClientTestSuite) TestWorkflowBatchUpdateOptions_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchUpdateOptionsClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := env.Context()

			workflowType := testcore.RandomizeStr(t.Name())
			env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

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

			// Apply a VersioningOverride to all three workflows with a single
			// batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_UpdateWorkflowOptionsOperation{
					UpdateWorkflowOptionsOperation: &batchpb.BatchOperationUpdateWorkflowExecutionOptions{
						Identity: "batch-updater",
						WorkflowExecutionOptions: &workflowpb.WorkflowExecutionOptions{
							VersioningOverride: &workflowpb.VersioningOverride{
								Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
							},
						},
						UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"versioning_override"}},
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_UPDATE_WORKFLOW_EXECUTION_OPTIONS, expectedQuery, expectedExecutions)

			// All three workflows must have the VersioningOverride applied.
			for _, e := range executions {
				//nolint:forbidigo // for tests with waits
				require.Eventually(t, func() bool {
					desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: env.Namespace().String(),
						Execution: e,
					})
					if err != nil {
						return false
					}
					override := desc.GetWorkflowExecutionInfo().GetVersioningInfo().GetVersioningOverride()
					autoUpgrade, ok := override.GetOverride().(*workflowpb.VersioningOverride_AutoUpgrade)
					return ok && autoUpgrade.AutoUpgrade
				}, 5*time.Second, 100*time.Millisecond)
			}
		})
	}
}
