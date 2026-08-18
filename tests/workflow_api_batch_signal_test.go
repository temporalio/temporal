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

// twoSignalReceivingWorkflow blocks until it receives batchSignalTestSignalName
// twice, then returns both payloads joined by a comma.
func twoSignalReceivingWorkflow(ctx workflow.Context) (string, error) {
	ch := workflow.GetSignalChannel(ctx, batchSignalTestSignalName)
	var first, second string
	ch.Receive(ctx, &first)
	ch.Receive(ctx, &second)
	return first + "," + second, nil
}

// TestWorkflowBatchSignal_SeparateJobsNotDeduped verifies that two distinct
// batch jobs signaling the same workflow with the same signal name both take
// effect. The batcher derives a deterministic request ID per call so that its
// own retries dedupe server-side; that request ID must be scoped to the batch
// job, otherwise the second job's signal is silently dropped as a duplicate.
func (s *WorkflowAPIBatchSignalClientTestSuite) TestWorkflowBatchSignal_SeparateJobsNotDeduped() {
	env := newWorkflowBatchEnv(s.T())
	t := s.T()
	ctx := s.Context()

	workflowType := testcore.RandomizeStr(t.Name())
	env.SdkWorker().RegisterWorkflowWithOptions(twoSignalReceivingWorkflow, workflow.RegisterOptions{Name: workflowType})

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr(t.Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowType)
	s.NoError(err)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}

	// Two separate batch jobs, same signal name, same target workflow.
	first := s.signalWorkflowViaBatch(ctx, env, execution, "first-signal-data")
	assertWorkflowBatchOperationSucceeded(ctx, t, env, first)

	second := s.signalWorkflowViaBatch(ctx, env, execution, "second-signal-data")
	assertWorkflowBatchOperationSucceeded(ctx, t, env, second)

	// Both signals must reach the workflow: if the second one is deduped, the
	// workflow stays blocked on its second Receive and never completes.
	getCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	var result string
	s.NoError(env.SdkClient().GetWorkflow(ctx, execution.GetWorkflowId(), execution.GetRunId()).Get(getCtx, &result))
	s.Equal("first-signal-data,second-signal-data", result)
}

// signalWorkflowViaBatch starts a batch signal operation targeting a single
// workflow execution and returns the batch job ID.
func (s *WorkflowAPIBatchSignalClientTestSuite) signalWorkflowViaBatch(
	ctx context.Context,
	env *testcore.TestEnv,
	execution *commonpb.WorkflowExecution,
	signalData string,
) string {
	inputPayloads, err := converter.GetDefaultDataConverter().ToPayloads(signalData)
	s.NoError(err)

	jobID := uuid.NewString()
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_SignalOperation{
			SignalOperation: &batchpb.BatchOperationSignal{
				Signal:   batchSignalTestSignalName,
				Input:    inputPayloads,
				Identity: "batch-signaler",
			},
		},
		TargetExecutions: toTargetExecutions([]*commonpb.WorkflowExecution{execution}),
		JobId:            jobID,
		Reason:           "test",
	})
	s.NoError(err)
	return jobID
}

// assertWorkflowBatchOperationSucceeded waits for the batch job to complete
// with exactly one successful operation and no failures.
func assertWorkflowBatchOperationSucceeded(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	jobID string,
) {
	t.Helper()

	//nolint:forbidigo // for tests with waits
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.Equal(c, enumspb.BATCH_OPERATION_STATE_COMPLETED, desc.GetState())
		require.EqualValues(c, 1, desc.GetCompleteOperationCount())
		require.Zero(c, desc.GetFailureOperationCount())
	}, 30*time.Second, 200*time.Millisecond)
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
