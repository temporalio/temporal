package tests

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type WorkflowAPIBatchResetClientTestSuite struct {
	parallelsuite.Suite[*WorkflowAPIBatchResetClientTestSuite]
}

func TestWorkflowAPIBatchResetClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkflowAPIBatchResetClientTestSuite{})
}

// newResetTestActivityAndWorkflow returns an activity/workflow pair, sharing
// one closure over attemptCounts, so the activity function registered with
// the worker is the exact same value the workflow invokes.
//
// The activity fails on a workflow's first attempt (tracked in attemptCounts,
// keyed by workflow ID so multiple concurrently-running executions don't
// interfere with each other) and succeeds thereafter. Resetting the workflow
// to its first workflow task re-runs the activity from scratch, so bumping
// the counter before the reset proves the reset actually happened (the second
// run succeeds).
func newResetTestActivityAndWorkflow(attemptCounts *sync.Map) (func(ctx context.Context) (int32, error), func(ctx workflow.Context) (int32, error)) {
	activityFn := func(ctx context.Context) (int32, error) {
		wfID := activity.GetInfo(ctx).WorkflowExecution.ID
		counterI, _ := attemptCounts.LoadOrStore(wfID, new(atomic.Int32))
		counter := counterI.(*atomic.Int32)
		if val := counter.Load(); val != 0 {
			return val, nil
		}
		return 0, temporal.NewApplicationError("some random error", "", false, nil)
	}
	workflowFn := func(ctx workflow.Context) (int32, error) {
		ao := workflow.ActivityOptions{
			ScheduleToStartTimeout: 20 * time.Second,
			StartToCloseTimeout:    40 * time.Second,
		}
		ctx = workflow.WithActivityOptions(ctx, ao)

		var result int32
		err := workflow.ExecuteActivity(ctx, activityFn).Get(ctx, &result)
		return result, err
	}
	return activityFn, workflowFn
}

func (s *WorkflowAPIBatchResetClientTestSuite) TestWorkflowBatchReset_Success() {
	for _, selector := range workflowBatchTargetSelectors() {
		s.Run(selector.name, func(s *WorkflowAPIBatchResetClientTestSuite) {
			env := newWorkflowBatchEnv(s.T())
			t := s.T()
			ctx := s.Context()

			var attemptCounts sync.Map
			activityFn, workflowFn := newResetTestActivityAndWorkflow(&attemptCounts)
			workflowType := testcore.RandomizeStr(t.Name())
			env.SdkWorker().RegisterWorkflowWithOptions(workflowFn, workflow.RegisterOptions{Name: workflowType})
			env.SdkWorker().RegisterActivity(activityFn)

			// Start three workflows of the same (per-test, unique) type; each
			// fails on its first attempt.
			executions := make([]*commonpb.WorkflowExecution, 0, 3)
			for i := range 3 {
				run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
					ID:                       testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i)),
					TaskQueue:                env.WorkerTaskQueue(),
					WorkflowExecutionTimeout: 10 * time.Second,
				}, workflowType)
				s.NoError(err)

				var result int32
				s.Error(run.Get(ctx, &result))
				executions = append(executions, &commonpb.WorkflowExecution{
					WorkflowId: run.GetID(),
					RunId:      run.GetRunID(),
				})
			}

			// Bump each workflow's attempt counter so the activity succeeds on
			// the next (post-reset) attempt.
			for _, e := range executions {
				counterI, _ := attemptCounts.LoadOrStore(e.GetWorkflowId(), new(atomic.Int32))
				counterI.(*atomic.Int32).Add(1)
			}

			// Reset all three workflows to their first workflow task with a
			// single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_ResetOperation{
					ResetOperation: &batchpb.BatchOperationReset{
						ResetType: enumspb.RESET_TYPE_FIRST_WORKFLOW_TASK,
						Identity:  "batch-resetter",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, workflowType, executions, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type, query, and executions for the batch.
			assertWorkflowBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_RESET_WORKFLOW, expectedQuery, expectedExecutions)

			// The reset run of each workflow must now complete successfully.
			for _, e := range executions {
				//nolint:forbidigo // for tests with waits
				require.Eventually(t, func() bool {
					var result int32
					run := env.SdkClient().GetWorkflow(ctx, e.GetWorkflowId(), "")
					err := run.Get(ctx, &result)
					return err == nil && result == 1
				}, 10*time.Second, 200*time.Millisecond)
			}
		})
	}
}
