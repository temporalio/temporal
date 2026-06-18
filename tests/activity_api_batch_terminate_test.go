package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	batchpb "go.temporal.io/api/batch/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
)

type ActivityAPIBatchTerminateClientTestSuite struct {
	parallelsuite.Suite[*ActivityAPIBatchTerminateClientTestSuite]
}

func TestActivityAPIBatchTerminateClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIBatchTerminateClientTestSuite{})
}

// newStandaloneActivityBatchEnv builds a standalone-activity test env that also
// has the batcher worker service running so that activity batch operations
// (terminate/cancel/delete) can be exercised end-to-end. It mirrors
// standaloneActivityTestSuite.newTestEnv (enabling the CHASM/activity feature
// flags) and additionally enables the "batch operations" worker service and
// raises the per-namespace concurrent batch limit to the functional-test limit.
func newStandaloneActivityBatchEnv(t *testing.T) *standaloneActivityEnv {
	env := &standaloneActivityEnv{
		TestEnv: testcore.NewEnv(
			t,
			testcore.WithWorkerService("batch operations"),
			// These tests intentionally start multiple batch operations in the same namespace.
			// The default per-namespace limit is 1, so raise it to the functional test limit.
			testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit),
		),
	}
	nsValues := func(value any) []dynamicconfig.ConstrainedValue {
		return []dynamicconfig.ConstrainedValue{
			{Constraints: dynamicconfig.Constraints{Namespace: env.Namespace().String()}, Value: value},
			{Constraints: dynamicconfig.Constraints{Namespace: env.ExternalNamespace().String()}, Value: value},
		}
	}
	cluster := env.GetTestCluster()
	cluster.OverrideDynamicConfig(t, dynamicconfig.EnableChasm, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.Enabled, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.EnableCallbacks, nsValues(true))
	cluster.OverrideDynamicConfig(t, activity.StartDelayEnabled, nsValues(true))
	return env
}

// startedActivity tracks a standalone activity that has been started so that it
// can be looked up after a batch operation runs.
type startedActivity struct {
	activityID string
	runID      string
}

func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_Success() {
	env := newStandaloneActivityBatchEnv(s.T())
	t := s.T()
	ctx := env.Context()

	activityType := env.Tv().ActivityType().GetName()
	taskQueue := testcore.RandomizeStr(t.Name())

	// Start three standalone activities of the same (per-test, unique) type.
	activities := make([]startedActivity, 0, 3)
	for i := 0; i < 3; i++ {
		activityID := testcore.RandomizeStr(fmt.Sprintf("%s-%d", t.Name(), i))
		startResp := env.startAndValidateActivity(ctx, t, activityID, taskQueue)
		activities = append(activities, startedActivity{activityID: activityID, runID: startResp.RunId})
	}

	// All three activities are Running; scope the batch to exactly these via execution status + type.
	query := fmt.Sprintf("ExecutionStatus = 'Running' AND ActivityType = '%s'", activityType)

	// Wait for the activities to be indexed in visibility before submitting the batch.
	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.Len(t, listResp.GetExecutions(), 3)
	}, testcore.WaitForESToSettle, 100*time.Millisecond)

	// Terminate all three activities with a single batch operation.
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		VisibilityQuery: query,
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	// All three activities must reach the Terminated status.
	for _, a := range activities {
		env.eventuallyTerminated(ctx, t, a.activityID, a.runID)
	}
}
