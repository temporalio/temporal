package tests

import (
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

type ActivityAPIBatchDeleteClientTestSuite struct {
	parallelsuite.Suite[*ActivityAPIBatchDeleteClientTestSuite]
}

func TestActivityAPIBatchDeleteClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIBatchDeleteClientTestSuite{})
}

func (s *ActivityAPIBatchDeleteClientTestSuite) TestActivityBatchDelete_Success() {
	for _, selector := range batchTargetSelectors() {
		s.Run(selector.name, func(s *ActivityAPIBatchDeleteClientTestSuite) {
			env := newStandaloneActivityBatchEnv(s.T())
			t := s.T()
			ctx := s.Context()

			activityType := env.Tv().ActivityType().GetName()
			taskQueue := testcore.RandomizeStr(t.Name())

			// Start three standalone activities of the same (per-test, unique) type.
			activities := make([]startedActivity, 0, 3)
			for i := range 3 {
				activityID := testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i))
				startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
				activities = append(activities, startedActivity{activityID: activityID, runID: startResp.RunId})
			}

			// Delete all three activities with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
					DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, activityType, activities, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type for the batch.
			assertBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY, expectedQuery, expectedExecutions)

			// All three activities must be deleted (no longer describable).
			for _, a := range activities {
				env.eventuallyDeleted(ctx, t, a.activityID, a.runID)
			}
		})
	}
}

// TestActivityBatchDelete_IncludesNonRunning verifies that, unlike batch
// terminate/cancel, batch delete does NOT add an `ExecutionStatus = 'Running'`
// filter: an already-completed activity of the same type must still be
// targeted (and deleted) by a query scoped only by ActivityType.
func (s *ActivityAPIBatchDeleteClientTestSuite) TestActivityBatchDelete_IncludesNonRunning() {
	env := newStandaloneActivityBatchEnv(s.T())
	t := s.T()
	ctx := s.Context()

	activityType := env.Tv().ActivityType().GetName()

	// Distinct task queues per activity so polling for the completed activity's
	// task can't race and pick up the running activity's task instead.
	runningID := testcore.RandomizeStr(fmt.Sprintf("%s-running", t.Name()))
	runningResp := env.startAndValidateActivity(ctx, t, runningID, testcore.RandomizeStr(fmt.Sprintf("%s-running-tq", t.Name())))

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

	// Query intentionally omits ExecutionStatus = 'Running'; delete must not
	// add it, so both activities are targeted.
	query := fmt.Sprintf("ActivityType = '%s'", activityType)

	// Wait for both activities (one Running, one Completed) to be indexed.
	//nolint:forbidigo // for tests with waits
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
		Operation: &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
			DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
		},
		VisibilityQuery: query,
		JobId:           jobID,
		Reason:          "test",
	})
	s.NoError(err)

	// No running-status filter is added for delete, so both executions are
	// targeted -- not just the running one.
	//nolint:forbidigo // for tests with waits
	s.EventuallyWithT(func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.EqualValues(c, 2, desc.GetTotalOperationCount())
	}, 10*time.Second, 200*time.Millisecond)

	env.eventuallyDeleted(ctx, t, runningID, runningResp.RunId)
	env.eventuallyDeleted(ctx, t, completedID, completedResp.RunId)
}
