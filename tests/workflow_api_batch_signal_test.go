package tests

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchSignalClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchSignalClientTestSuite]
}

func TestWorkflowAPIBatchSignalClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchSignalClientTestSuite{})
}

const batchSignalTestSignalName = "workflow-batch-signal-test-signal"

// signalReceivingWorkflow blocks until it receives batchSignalTestSignalName,
// then returns the signal's data as the workflow result.
func signalReceivingWorkflow(ctx workflow.Context) (string, error) {
	var received string
	workflow.GetSignalChannel(ctx, batchSignalTestSignalName).Receive(ctx, &received)
	return received, nil
}

func (s *WorkflowAPIBatchSignalClientTestSuite) TestWorkflowBatchSignal_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchSignalClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := s.Context()

			workflowType := testcore.RandomizeStr(t.Name())
			env.SdkWorker().RegisterWorkflowWithOptions(signalReceivingWorkflow, workflow.RegisterOptions{Name: workflowType})

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

			signalData := testcore.RandomizeStr("signal-data")
			inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(signalData)
			s.NoError(err)

			// Signal all three workflows with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
					SignalOperation: &batchpb.BatchOperationSignal{
						Signal:   batchSignalTestSignalName,
						Input:    inputPayloads,
						Identity: "batch-signaler",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err = env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_SIGNAL_WORKFLOW, expectedQuery, expectedExecutions)

			// All three workflows must receive the signal and complete with its data.
			for _, e := range executions {
				var result string
				s.NoError(env.SdkClient().GetWorkflow(ctx, e.GetWorkflowId(), e.GetRunId()).Get(ctx, &result))
				s.Equal(signalData, result)
			}
		})
	}
}
