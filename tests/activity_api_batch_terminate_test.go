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
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/chasm/lib/activity"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
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
	return newStandaloneActivityBatchEnvWithOperators(t, true)
}

func newStandaloneActivityBatchEnvWithOperators(t *testing.T, enableBatchActivityOperators bool) *standaloneActivityEnv {
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
	if enableBatchActivityOperators {
		cluster.OverrideDynamicConfig(t, dynamicconfig.FrontendEnableBatchActivityOperators, nsValues(true))
	}
	return env
}

// startedActivity tracks a standalone activity that has been started so that it
// can be looked up after a batch operation runs.
type startedActivity struct {
	activityID string
	runID      string
}

// assertBatchOperationType verifies that both DescribeBatchOperation and
// ListBatchOperations report the expected operation type for the given batch job.
func assertBatchOperationType(
	ctx context.Context,
	t *testing.T,
	env *standaloneActivityEnv,
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

// batchTargetSelector describes a way to scope a batch operation's targets:
// a visibility query, the deprecated Executions field, or TargetExecutions.
type batchTargetSelector struct {
	name string
	// apply sets either VisibilityQuery or TargetExecutions on req to target
	// the given activities.
	apply func(t *testing.T, env *standaloneActivityEnv, ctx context.Context, activityType string, activities []startedActivity, req *workflowservice.StartBatchOperationRequest) (expectedQuery string, expectedExecutions []*commonpb.Execution)
}

// batchTargetSelectors enumerates the ways a caller can scope an activity
// batch operation's targets.
func batchTargetSelectors() []batchTargetSelector {
	return []batchTargetSelector{
		{
			name: "VisibilityQuery",
			apply: func(t *testing.T, env *standaloneActivityEnv, ctx context.Context, activityType string, activities []startedActivity, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				// ExecutionStatus = 'Running' is intentionally omitted: for
				// terminate/cancel the server adds it automatically
				// (adjustQueryBatchTypeEnum), and delete doesn't need it since
				// all activities here are still running anyway.
				query := fmt.Sprintf("ActivityType = '%s'", activityType)

				// Wait for the activities to be indexed in visibility before submitting the batch.
				//nolint:forbidigo // for tests with waits
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					listResp, err := env.FrontendClient().ListActivityExecutions(ctx, &workflowservice.ListActivityExecutionsRequest{
						Namespace: env.Namespace().String(),
						PageSize:  10,
						Query:     query,
					})
					require.NoError(c, err)
					require.Len(c, listResp.GetExecutions(), len(activities))
				}, testcore.WaitForESToSettle, 100*time.Millisecond)

				req.VisibilityQuery = query
				return query, nil
			},
		},
		{
			name: "Executions",
			apply: func(t *testing.T, env *standaloneActivityEnv, ctx context.Context, activityType string, activities []startedActivity, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				legacyExecutions := make([]*commonpb.WorkflowExecution, 0, len(activities))
				expectedExecutions := make([]*commonpb.Execution, 0, len(activities))
				for _, a := range activities {
					legacyExecutions = append(legacyExecutions, &commonpb.WorkflowExecution{
						WorkflowId: a.activityID,
						RunId:      a.runID,
					})
					expectedExecutions = append(expectedExecutions, &commonpb.Execution{
						Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
						BusinessId: a.activityID,
						RunId:      a.runID,
					})
				}
				//nolint:staticcheck // Exercises rollout compatibility for in-flight activity batches.
				req.Executions = legacyExecutions
				return "", expectedExecutions
			},
		},
		{
			name: "TargetExecutions",
			apply: func(t *testing.T, env *standaloneActivityEnv, ctx context.Context, activityType string, activities []startedActivity, req *workflowservice.StartBatchOperationRequest) (string, []*commonpb.Execution) {
				executions := make([]*commonpb.Execution, 0, len(activities))
				for _, a := range activities {
					executions = append(executions, &commonpb.Execution{
						Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
						BusinessId: a.activityID,
						RunId:      a.runID,
					})
				}
				req.TargetExecutions = executions
				return "", executions
			},
		},
	}
}

func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_Success() {
	for _, selector := range batchTargetSelectors() {
		s.Run(selector.name, func(s *ActivityAPIBatchTerminateClientTestSuite) {
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

			// Terminate all three activities with a single batch operation.
			jobID := uuid.NewString()
			req := &workflowservice.StartBatchOperationRequest{
				Namespace: env.Namespace().String(),
				Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
					TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
						Identity: "batch-terminator",
						Reason:   "test",
					},
				},
				JobId:  jobID,
				Reason: "test",
			}
			expectedQuery, expectedExecutions := selector.apply(t, env, ctx, activityType, activities, req)

			_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
			s.NoError(err)

			// Describe/List should report the correct operation type for the batch.
			assertBatchOperationType(ctx, t, env, jobID, enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, expectedQuery, expectedExecutions)

			// All three activities must reach the Terminated status.
			for _, a := range activities {
				env.eventuallyTerminated(ctx, t, a.activityID, a.runID)
			}
		})
	}
}

// TestActivityBatchTerminate_TargetExecutionTypeMismatch verifies that an
// activity-targeting batch operation (terminate activity) rejects
// TargetExecutions whose Type is EXECUTION_TYPE_WORKFLOW instead of
// silently misinterpreting the workflow's business_id/run_id as an activity
// execution and no-oping.
func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_TargetExecutionTypeMismatch() {
	env := newStandaloneActivityBatchEnv(s.T())
	ctx := s.Context()

	req := &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
		TargetExecutions: []*commonpb.Execution{
			{
				Type:       enumspb.EXECUTION_TYPE_WORKFLOW,
				BusinessId: "some-workflow-id",
				RunId:      uuid.NewString(),
			},
		},
	}

	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, req)
	s.Error(err)
	s.Contains(err.Error(), "target_executions[0]")
}

func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchOperatorsDisabled() {
	env := newStandaloneActivityBatchEnvWithOperators(s.T(), false)
	ctx := s.Context()

	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:       env.Namespace().String(),
		VisibilityQuery: "ActivityType = 'activity-type'",
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)
	s.ErrorContains(err, "not supported for this namespace")
}

func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_InFlightLegacyExecutionsContinueAfterDisable() {
	env := newStandaloneActivityBatchEnvWithOperators(s.T(), false)
	ctx := s.Context()
	disableOperators := env.OverrideDynamicConfig(dynamicconfig.FrontendEnableBatchActivityOperators, true)

	activityID := testcore.RandomizeStr(s.T().Name())
	startResp := env.startAndValidateActivity(ctx, s.T(), activityID, testcore.RandomizeStr(s.T().Name()))
	jobID := uuid.NewString()
	legacyExecutions := []*commonpb.WorkflowExecution{{WorkflowId: activityID, RunId: startResp.GetRunId()}}
	request := &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		JobId:  jobID,
		Reason: "test",
	}
	//nolint:staticcheck // Exercises an in-flight batch created with the deprecated selector.
	request.Executions = legacyExecutions
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, request)
	s.NoError(err)

	disableOperators()
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:       env.Namespace().String(),
		VisibilityQuery: "ActivityType = 'activity-type'",
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		JobId:  uuid.NewString(),
		Reason: "test",
	})
	var invalidArgument *serviceerror.InvalidArgument
	s.ErrorAs(err, &invalidArgument)

	assertBatchOperationType(ctx, s.T(), env, jobID, enumspb.BATCH_OPERATION_TYPE_TERMINATE_ACTIVITY, "", []*commonpb.Execution{{
		Type:       enumspb.EXECUTION_TYPE_ACTIVITY,
		BusinessId: activityID,
		RunId:      startResp.GetRunId(),
	}})
	env.eventuallyTerminated(ctx, s.T(), activityID, startResp.GetRunId())
}

func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_NoMatchingActivitiesCompletes() {
	env := newStandaloneActivityBatchEnv(s.T())
	jobID := uuid.NewString()
	ctx := s.Context()

	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace:       env.Namespace().String(),
		VisibilityQuery: fmt.Sprintf("ActivityId = '%s'", uuid.NewString()),
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
				Reason:   "test",
			},
		},
		JobId:  jobID,
		Reason: "test",
	})
	s.NoError(err)

	//nolint:forbidigo // for tests with waits
	s.EventuallyWithT(func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.Equal(c, enumspb.BATCH_OPERATION_STATE_COMPLETED, desc.GetState())
		require.Zero(c, desc.GetTotalOperationCount())
	}, 10*time.Second, 200*time.Millisecond)
}

// TestActivityBatchTerminate_ExcludesNonRunning verifies that a batch terminate
// query omitting `ExecutionStatus = 'Running'` still only targets the running
// activity: the server must add the filter automatically (adjustQueryBatchTypeEnum)
// so an already-completed activity of the same type is excluded, not (re-)terminated.
func (s *ActivityAPIBatchTerminateClientTestSuite) TestActivityBatchTerminate_ExcludesNonRunning() {
	env := newStandaloneActivityBatchEnv(s.T())
	t := s.T()
	ctx := s.Context()

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
		Operation: &workflowservice.StartBatchOperationRequest_TerminateActivitiesOperation{
			TerminateActivitiesOperation: &batchpb.BatchOperationTerminateActivities{
				Identity: "batch-terminator",
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
	//nolint:forbidigo // for tests with waits
	s.EventuallyWithT(func(c *assert.CollectT) {
		desc, err := env.FrontendClient().DescribeBatchOperation(ctx, &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		require.NoError(c, err)
		require.EqualValues(c, 1, desc.GetTotalOperationCount())
	}, 10*time.Second, 200*time.Millisecond)

	env.eventuallyTerminated(ctx, t, runningID, runningResp.RunId)

	// The already-completed activity must remain untouched by the batch.
	descResp, err := env.FrontendClient().DescribeActivityExecution(ctx, &workflowservice.DescribeActivityExecutionRequest{
		Namespace:  env.Namespace().String(),
		ActivityId: completedID,
		RunId:      completedResp.RunId,
	})
	s.NoError(err)
	s.Equal(enumspb.ACTIVITY_EXECUTION_STATUS_COMPLETED, descResp.GetInfo().GetStatus())
}
