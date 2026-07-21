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
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchTerminateClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchTerminateClientTestSuite]
}

func TestWorkflowAPIBatchTerminateClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchTerminateClientTestSuite{})
}

// newWorkflowBatchEnv builds a test env with the "batch operations" worker
// service running so that workflow batch operations (terminate/cancel/signal/
// delete/reset/update options) can be exercised end-to-end. It raises the
// per-namespace concurrent batch limit, since these tests intentionally start
// multiple batch operations in the same namespace (the default limit is 1).
func newWorkflowBatchEnv(t *testing.T) *testcore.TestEnv {
	return testcore.NewEnv(
		t,
		testcore.WithWorkerService("batch operations"),
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit),
	)
}

// assertWorkflowBatchOperationType verifies that both DescribeBatchOperation
// and ListBatchOperations report the expected operation type for the given
// batch job, and that DescribeBatchOperation reports the expected query and
// executions the batch was started with (whichever selector was used to
// start it — the other is expected to be empty).
func assertWorkflowBatchOperationType(
	ctx context.Context,
	t *testing.T,
	env *testcore.TestEnv,
	jobID string,
	expected enumspb.BatchOperationType,
	expectedQuery string,
	expectedExecutions []*commonpb.Execution,
) {
	t.Helper()

	//nolint:forbidigo // for tests with waits
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.Equal(c, expected, desc.GetOperationType())
		require.Equal(c, expectedQuery, desc.GetQuery())
		// Proto messages carry internal cache fields (e.g. sizeCache) that can
		// differ between otherwise-identical messages depending on whether
		// they've been marshaled; compare by proto semantics, not reflection.
		protorequire.ProtoSliceEqual(c, expectedExecutions, desc.GetExecutions())
	}, 10*time.Second, 200*time.Millisecond)

	//nolint:forbidigo // for tests with waits
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := env.FrontendClient().ListBatchOperations(ctx, &workflowservice.ListBatchOperationsRequest{
			Namespace: env.Namespace().String(),
		})
		require.NoError(c, err)
		var found *batchpb.BatchOperationInfo
		for _, op := range resp.GetOperationInfo() {
			if op.GetJobId() == jobID {
				found = op
				break
			}
		}
		require.NotNil(c, found, "job %s not found in ListBatchOperations", jobID)
		require.Equal(c, expected, found.GetOperationType())
	}, 10*time.Second, 200*time.Millisecond)
}

// workflowBatchTargetSelector describes a way to scope a workflow batch
// operation's targets: a visibility query, the deprecated Executions field, or
// the new TargetExecutions field.
type workflowBatchTargetSelector struct {
	name string
	// apply sets one of VisibilityQuery, Executions, or TargetExecutions on
	// req to target the given executions, and returns the query/executions
	// DescribeBatchOperation is expected to report back (see
	// assertWorkflowBatchOperationType) — whichever of the two this selector
	// didn't set is expected to come back empty.
	apply func(t *testing.T, env *testcore.TestEnv, ctx context.Context, workflowType string, executions []*commonpb.WorkflowExecution, req *workflowservice.StartBatchOperationRequest) (expectedQuery string, expectedExecutions []*commonpb.Execution)
}

// toTargetExecutions converts workflow executions into the unified
// Execution representation used by TargetExecutions and echoed back by
// DescribeBatchOperation.
func toTargetExecutions(executions []*commonpb.WorkflowExecution) []*commonpb.Execution {
	targetExecutions := make([]*commonpb.Execution, 0, len(executions))
	for _, e := range executions {
		targetExecutions = append(targetExecutions, &commonpb.Execution{
			Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
			BusinessId: e.GetWorkflowId(),
			RunId:      e.GetRunId(),
		})
	}
	return targetExecutions
}

// workflowBatchTargetSelectors enumerates the ways a caller can scope a
// workflow batch operation's targets, so the same test body can be run
// against all of them.
func workflowBatchTargetSelectors() []workflowBatchTargetSelector {
	return []workflowBatchTargetSelector{
		{
			name: "VisibilityQuery",
			apply: func(t *testing.T, env *testcore.TestEnv, ctx context.Context, workflowType string, executions []*commonpb.WorkflowExecution, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				query := fmt.Sprintf("WorkflowType = '%s'", workflowType)

				// Wait for the executions to be indexed in visibility before submitting the batch.
				//nolint:forbidigo // for tests with waits
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					listResp, err := env.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
						Namespace: env.Namespace().String(),
						PageSize:  10,
						Query:     query,
					})
					require.NoError(c, err)
					require.Len(c, listResp.GetExecutions(), len(executions))
				}, testcore.WaitForESToSettle, 100*time.Millisecond)

				req.VisibilityQuery = query
				return query, nil
			},
		},
		{
			name: "Executions",
			apply: func(t *testing.T, env *testcore.TestEnv, ctx context.Context, workflowType string, executions []*commonpb.WorkflowExecution, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				//nolint:staticcheck // SA1019: intentionally exercising the deprecated Executions selector
				req.Executions = executions
				return "", toTargetExecutions(executions)
			},
		},
		{
			name: "TargetExecutions",
			apply: func(t *testing.T, env *testcore.TestEnv, ctx context.Context, workflowType string, executions []*commonpb.WorkflowExecution, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				targetExecutions := toTargetExecutions(executions)
				req.TargetExecutions = targetExecutions
				return "", targetExecutions
			},
		},
	}
}

func (s *WorkflowAPIBatchTerminateClientTestSuite) TestWorkflowBatchTerminate_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchTerminateClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := s.Context()

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

			// Terminate all three workflows with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
					TerminationOperation: &batchpb.BatchOperationTermination{
						Identity: "batch-terminator",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_TERMINATE_WORKFLOW, expectedQuery, expectedExecutions)

			// All three workflows must reach the Terminated status.
			for _, e := range executions {
				//nolint:forbidigo // for tests with waits
				require.Eventually(t, func() bool {
					desc, err := env.FrontendClient().DescribeWorkflowExecution(ctx, &workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: env.Namespace().String(),
						Execution: e,
					})
					return err == nil && desc.GetWorkflowExecutionInfo().GetStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED
				}, 5*time.Second, 100*time.Millisecond)
			}
		})
	}
}

// TestWorkflowBatchTerminate_TargetExecutionTypeMismatch verifies that a
// workflow-targeting batch operation (e.g. terminate workflow) rejects
// TargetExecutions whose Type is EXECUTION_TYPE_ACTIVITY instead of
// silently misinterpreting the activity's business_id/run_id as a workflow
// execution and no-oping.
func (s *WorkflowAPIBatchTerminateClientTestSuite) TestWorkflowBatchTerminate_TargetExecutionTypeMismatch() {
	env := newWorkflowBatchEnv(s.T())
	ctx := s.Context()

	req := &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_TerminationOperation{
			TerminationOperation: &batchpb.BatchOperationTermination{
				Identity: "batch-terminator",
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
		TargetExecutions: []*commonpb.Execution{
			{
				Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
				BusinessId: "some-activity-id",
				RunId:      uuid.NewString(),
			},
		},
	}

	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
	s.Error(err)
	s.Contains(err.Error(), "target_executions[0]")
}
