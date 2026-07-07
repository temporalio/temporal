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

	// All three activities are Running; scope the batch to exactly these via execution status + type.
	query := fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType)

	// Wait for the activities to be indexed in visibility before submitting the batch.
	//nolint:forbidigo // for tests with waits
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, listResp.GetExecutions(), 3)
	}, testcore.WaitForESToSettle, 100*time.Millisecond)

	// Delete all three activities with a single batch operation.
	jobID := uuid.NewString()
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_DeleteActivitiesOperation{
			DeleteActivitiesOperation: &batchpb.BatchOperationDeleteActivities{},
		},
		VisibilityQuery: query,
		JobId:           jobID,
		Reason:          "test",
	})
	s.NoError(err)

	// Describe/List should report the correct operation type for the batch.
	assertBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_DELETE_ACTIVITY)

	// All three activities must be deleted (no longer describable).
	for _, a := range activities {
		env.eventuallyDeleted(ctx, t, a.activityID, a.runID)
	}
}
