package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type ActivityAPIBatchResetClientTestSuite struct {
	parallelsuite.Suite[*ActivityAPIBatchResetClientTestSuite]
}

func TestActivityAPIBatchResetClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIBatchResetClientTestSuite{})
}

func newBatchResetEnv(t *testing.T) *testcore.TestEnv {
	return testcore.NewEnv(
		t,
		testcore.WithWorkerService("batch operations"),
		// These tests intentionally start multiple batch operations in the same namespace.
		// The default per-namespace limit is 1, so raise it to the functional test limit.
		testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit),
	)
}

func (s *ActivityAPIBatchResetClientTestSuite) createBatchResetWorkflow(env *testcore.TestEnv, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id"),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(env.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityAPIBatchResetClientTestSuite) TestActivityBatchReset_Success() {
	env := newBatchResetEnv(s.T())

	internalWorkflow := newInternalWorkflow()

	env.SdkWorker().RegisterWorkflow(internalWorkflow.WorkflowFunc)
	env.SdkWorker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRun1 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)
	workflowRun2 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)

	// wait for activity to start in both workflows
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activities in both workflows
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}

	pauseRequest.Execution.WorkflowId = workflowRun1.GetID()
	resp, err := env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
	resp, err = env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activities to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	workflowTypeName := "WorkflowFunc"
	activityTypeName := "ActivityFunc"

	// Make sure the activity is in visibility
	var listResp *workflowservice.ListWorkflowExecutionsResponse
	searchValue := fmt.Sprintf("property:activityType=%s", activityTypeName)
	escapedSearchValue := sqlparser.String(sqlparser.NewStrVal([]byte(searchValue)))
	resetCause := fmt.Sprintf("%s = %s", sadefs.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = env.FrontendClient().ListWorkflowExecutions(env.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: true,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(env.Context(), &out)
	s.NoError(err)

	err = workflowRun2.Get(env.Context(), &out)
	s.NoError(err)
}

func (s *ActivityAPIBatchResetClientTestSuite) TestActivityBatchReset_Success_Protobuf() {
	env := newBatchResetEnv(s.T())

	internalWorkflow := newInternalWorkflow()

	env.SdkWorker().RegisterWorkflow(internalWorkflow.WorkflowFunc)
	env.SdkWorker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRun1 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)
	workflowRun2 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)

	// wait for activity to start in both workflows
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activities in both workflows
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}

	pauseRequest.Execution.WorkflowId = workflowRun1.GetID()
	resp, err := env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
	resp, err = env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activities to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	workflowTypeName := "WorkflowFunc"
	activityTypeName := "ActivityFunc"

	// Make sure the activity is in visibility
	var listResp *workflowservice.ListWorkflowExecutionsResponse
	searchValue := fmt.Sprintf("property:activityType=%s", activityTypeName)
	escapedSearchValue := sqlparser.String(sqlparser.NewStrVal([]byte(searchValue)))
	resetCause := fmt.Sprintf("%s = %s", sadefs.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = env.FrontendClient().ListWorkflowExecutions(env.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: true,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(env.Context(), &out)
	s.NoError(err)

	err = workflowRun2.Get(env.Context(), &out)
	s.NoError(err)
}

func (s *ActivityAPIBatchResetClientTestSuite) TestActivityBatchReset_RunningWorkflowsResetAttempts() {
	env := newBatchResetEnv(s.T())
	ctx := env.Context()

	const workflowCount = 10
	workflowTypeName := testcore.RandomizeStr("activity-batch-reset-running-workflow")

	internalWorkflow := newInternalWorkflow()
	internalWorkflow.initialRetryInterval = 100 * time.Millisecond
	internalWorkflow.activityRetryPolicy.InitialInterval = internalWorkflow.initialRetryInterval

	env.SdkWorker().RegisterWorkflowWithOptions(internalWorkflow.WorkflowFunc, workflow.RegisterOptions{Name: workflowTypeName})
	env.SdkWorker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRuns := make([]sdkclient.WorkflowRun, 0, workflowCount)
	for i := 0; i < workflowCount; i++ {
		workflowRun, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
			ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
			TaskQueue: env.WorkerTaskQueue(),
		}, workflowTypeName)
		s.NoError(err)
		s.NotNil(workflowRun)
		workflowRuns = append(workflowRuns, workflowRun)
	}

	s.Await(func(s *ActivityAPIBatchResetClientTestSuite) {
		for _, workflowRun := range workflowRuns {
			description, err := env.SdkClient().DescribeWorkflowExecution(s.Context(), workflowRun.GetID(), workflowRun.GetRunID())
			s.NoError(err)
			s.Len(description.PendingActivities, 1)
			s.Greater(description.PendingActivities[0].Attempt, int32(3))
		}
	}, 15*time.Second, 100*time.Millisecond)

	env.SdkWorker().Stop()

	query := fmt.Sprintf("WorkflowType='%s' AND ExecutionStatus = 'Running'", workflowTypeName)
	s.Await(func(s *ActivityAPIBatchResetClientTestSuite) {
		listResp, err := env.FrontendClient().ListWorkflowExecutions(s.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  workflowCount,
			Query:     query,
		})
		s.NoError(err)
		s.Len(listResp.GetExecutions(), workflowCount)
	}, 5*time.Second, 500*time.Millisecond)

	jobID := uuid.NewString()
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(ctx, &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				ResetAttempts:  true,
				ResetHeartbeat: true,
				Activity:       &batchpb.BatchOperationResetActivities_MatchAll{MatchAll: true},
			},
		},
		VisibilityQuery: query,
		JobId:           jobID,
		Reason:          "test",
	})
	s.NoError(err)

	s.Await(func(s *ActivityAPIBatchResetClientTestSuite) {
		descResp, err := env.FrontendClient().DescribeBatchOperation(s.Context(), &workflowservice.DescribeBatchOperationRequest{
			Namespace: env.Namespace().String(),
			JobId:     jobID,
		})
		s.NoError(err)
		s.Equal(enumspb.BATCH_OPERATION_STATE_COMPLETED, descResp.GetState())
	}, 15*time.Second, 100*time.Millisecond)

	for _, workflowRun := range workflowRuns {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun.GetID(), workflowRun.GetRunID())
		s.NoError(err)
		s.Len(description.PendingActivities, 1)
		s.Equal(int32(1), description.PendingActivities[0].Attempt)
	}

	internalWorkflow.letActivitySucceed.Store(true)

	replacementWorker := sdkworker.New(env.SdkClient(), env.WorkerTaskQueue(), sdkworker.Options{})
	replacementWorker.RegisterWorkflowWithOptions(internalWorkflow.WorkflowFunc, workflow.RegisterOptions{Name: workflowTypeName})
	replacementWorker.RegisterActivity(internalWorkflow.ActivityFunc)
	s.NoError(replacementWorker.Start())
	defer replacementWorker.Stop()

	for _, workflowRun := range workflowRuns {
		var out string
		err = workflowRun.Get(ctx, &out)
		s.NoError(err)
	}
}

func (s *ActivityAPIBatchResetClientTestSuite) TestActivityBatchReset_DontResetAttempts() {
	env := newBatchResetEnv(s.T())

	internalWorkflow := newInternalWorkflow()

	env.SdkWorker().RegisterWorkflow(internalWorkflow.WorkflowFunc)
	env.SdkWorker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRun1 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)
	workflowRun2 := s.createBatchResetWorkflow(env, internalWorkflow.WorkflowFunc)

	// wait for activity to start in both workflows
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 100*time.Millisecond)

	// pause activities in both workflows
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}

	pauseRequest.Execution.WorkflowId = workflowRun1.GetID()
	resp, err := env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
	resp, err = env.FrontendClient().PauseActivity(env.Context(), pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activities to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	workflowTypeName := "WorkflowFunc"
	activityTypeName := "ActivityFunc"

	// Make sure the activity is in visibility
	var listResp *workflowservice.ListWorkflowExecutionsResponse
	searchValue := fmt.Sprintf("property:activityType=%s", activityTypeName)
	escapedSearchValue := sqlparser.String(sqlparser.NewStrVal([]byte(searchValue)))
	resetCause := fmt.Sprintf("%s = %s", sadefs.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = env.FrontendClient().ListWorkflowExecutions(env.Context(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:      &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused:    false,
				ResetAttempts: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotEqual(t, int32(1), description.PendingActivities[0].Attempt)

		description, err = env.SdkClient().DescribeWorkflowExecution(env.Context(), workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotEqual(t, int32(1), description.PendingActivities[0].Attempt)
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(env.Context(), &out)
	s.NoError(err)

	err = workflowRun2.Get(env.Context(), &out)
	s.NoError(err)
}

func (s *ActivityAPIBatchResetClientTestSuite) TestActivityBatchReset_Failed() {
	env := newBatchResetEnv(s.T())

	// neither activity type not "match all" is provided
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// neither activity type not "match all" is provided
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity: &batchpb.BatchOperationResetActivities_Type{Type: ""},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// malformed visibility query should be rejected before the batch workflow starts
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_DeletionOperation{
			DeletionOperation: &batchpb.BatchOperationDeletion{},
		},
		VisibilityQuery: "()",
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))
}
