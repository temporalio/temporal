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
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type ActivityAPIBatchCancelClientTestSuite struct {
	parallelsuite.Suite[*ActivityAPIBatchCancelClientTestSuite]
}

func TestActivityAPIBatchCancelClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIBatchCancelClientTestSuite{})
}

func (env *standaloneActivityEnv) eventuallyCanceled(ctx context.Context, t *testing.T, activityID, runID string) {
	t.Helper()
	//nolint:forbidigo // tests with waits
	require.Eventually(t, func() bool {
		resp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
			Namespace:  env.Namespace().String(),
			ActivityId: activityID,
			RunId:      runID,
		})
		return err == nil && resp.GetInfo().GetStatus() == enumspb.ACTIVITY_EXECUTION_STATUS_CANCELED
	}, 5*time.Second, 100*time.Millisecond)
}

func (s *ActivityAPIBatchCancelClientTestSuite) TestActivityBatchCancel_Success() {
	for _, selector := range batchTargetSelectors() {
		s.Run(selector.name, func(s *ActivityAPIBatchCancelClientTestSuite) {
			env := newStandaloneActivityBatchEnv(s.T())
			t := s.T()
			ctx := env.Context()

			activityType := env.Tv().ActivityType().GetName()
			taskQueue := testcore.RandomizeStr(t.Name())

			// Start three standalone activities of the same (per-test, unique) type.
			activities := make([]startedActivity, 0, 3)
			for i := range 3 {
				activityID := testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i))
				startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
				activities = append(activities, startedActivity{activityID: activityID, runID: startResp.RunId})
			}

			// Cancel all three activities with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
					CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{
						Identity: "batch-canceler",
						Reason:   "test",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			selector.apply(t, env, ctx, activityType, activities, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type for the batch.
			assertBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_CANCEL_ACTIVITY)

			// All three activities must reach the Canceled status.
			for _, a := range activities {
				env.eventuallyCanceled(ctx, t, a.activityID, a.runID)
			}
		})
	}
}

// TestActivityBatchCancel_ExcludesNonRunning verifies that a batch cancel query
// omitting `ExecutionStatus = 'Running'` still only targets the running
// activity: the server must add the filter automatically (adjustQueryBatchTypeEnum)
// so an already-completed activity of the same type is excluded, not (re-)canceled.
func (s *ActivityAPIBatchCancelClientTestSuite) TestActivityBatchCancel_ExcludesNonRunning() {
	env := newStandaloneActivityBatchEnv(s.T())
	t := s.T()
	ctx := env.Context()

	activityType := env.Tv().ActivityType().GetName()

	// Distinct task queues per activity so polling for the completed activity's
	// task can't race and pick up the running activity's task instead.
	runningID := testcore.RandomizeStr(fmt.Sprintf("%s-running", t.Name()))
	runningResp := env.startAndValidateActivity(ctx, t, runningID, testcore.RandomizeStr(fmt.Sprintf("%s-running-tq", t.Name())))

	// This activity completes normally before the batch runs, so it must be
	// excluded from a batch operation scoped only by ActivityType.
	completedTaskQueue := testcore.RandomizeStr(fmt.Sprintf("%s-completed-tq", t.Name()))
	completedID := testcore.RandomizeStr(fmt.Sprintf("%s-completed", t.Name()))
	completedResp := env.startAndValidateActivity(ctx, t, completedID, completedTaskQueue)
	pollResp := env.pollActivityTaskAndValidate(ctx, t, completedID, completedTaskQueue, completedResp.RunId)
	_, err := env.FrontendClient().RespondActivityTaskCompleted(ctx, &workflowservice.RespondActivityTaskCompletedRequest{
		Namespace: env.Namespace().String(),
		TaskToken: pollResp.TaskToken,
		Result:    defaultResult,
	})
	s.NoError(err)

	// Query intentionally omits ExecutionStatus = 'Running'.
	query := fmt.Sprintf("ActivityType = '%s'", activityType)

	// Wait for both activities (one Running, one Completed) to be indexed.
	//nolint:forbidigo // tests with waits
	s.EventuallyWithT(func(c *assert.CollectT) {
		listResp, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(c, err)
		require.Len(c, listResp.GetExecutions(), 2)
	}, testcore.WaitForESToSettle, 100*time.Millisecond)

	jobID := uuid.NewString()
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_CancelActivitiesOperation{
			CancelActivitiesOperation: &batchpb.BatchOperationCancelActivities{
				Identity: "batch-canceler",
				Reason:   "test",
			},
		},
		VisibilityQuery: query,
		JobId:           jobID,
		Reason:          "test",
	})
	s.NoError(err)

	// The server-added ExecutionStatus = 'Running' filter must exclude the
	// already-completed activity: only 1 execution is ever targeted, not 2.
	//nolint:forbidigo // tests with waits
	s.EventuallyWithT(func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.EqualValues(c, 1, desc.GetTotalOperationCount())
	}, 10*time.Second, 200*time.Millisecond)

	env.eventuallyCanceled(ctx, t, runningID, runningResp.RunId)

	// The already-completed activity must remain untouched by the batch.
	descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: completedID,
		RunId:      completedResp.RunId,
	})
	s.NoError(err)
	s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
}
