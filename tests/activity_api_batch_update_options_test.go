package tests

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	activitypb "go.temporal.io/api/activity/v1"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type ActivityAPIBatchUpdateOptionsSuite struct {
	parallelsuite.Suite[*ActivityAPIBatchUpdateOptionsSuite]
}

func TestActivityApiBatchUpdateOptionsClientTestSuite(t *testing.T) {
	parallelsuite.Run(t, &ActivityAPIBatchUpdateOptionsSuite{})
}

func (s *ActivityAPIBatchUpdateOptionsSuite) createBatchUpdateOptionsWorkflow(env *testcore.TestEnv, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: env.WorkerTaskQueue(),
	}
	workflowRun, err := env.SdkClient().ExecuteWorkflow(env.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityAPIBatchUpdateOptionsSuite) TestActivityBatchUpdateOptionsSuccess() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit))

	ctx := env.Context()

	internalWorkflow := newInternalWorkflow()

	env.SdkWorker().RegisterWorkflow(internalWorkflow.WorkflowFunc)
	env.SdkWorker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRun1 := s.createBatchUpdateOptionsWorkflow(env, internalWorkflow.WorkflowFunc)
	workflowRun2 := s.createBatchUpdateOptionsWorkflow(env, internalWorkflow.WorkflowFunc)

	// wait for activity to start in both workflows
	env.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Positive(t, internalWorkflow.startedActivityCount.Load())

		description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
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
	resp, err := env.FrontendClient().PauseActivity(ctx, pauseRequest)
	env.NoError(err)
	env.NotNil(resp)

	pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
	resp, err = env.FrontendClient().PauseActivity(ctx, pauseRequest)
	env.NoError(err)
	env.NotNil(resp)

	// wait for activities to be paused
	env.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
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
	unpauseCause := fmt.Sprintf("%s = %s", sadefs.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, unpauseCause)

	env.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = env.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: env.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
	}, 5*time.Second, 500*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{
				Activity: &batchpb.BatchOperationUpdateActivityOptions_Type{Type: activityTypeName},
				ActivityOptions: &activitypb.ActivityOptions{
					ScheduleToCloseTimeout: durationpb.New(10 * time.Second),
				},
				UpdateMask: &fieldmaskpb.FieldMask{
					Paths: []string{"schedule_to_close_timeout"},
				},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	env.NoError(err)

	// make sure activities are unpaused
	env.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, 10*time.Second, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration())
		require.True(t, description.PendingActivities[0].Paused)

		description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, 10*time.Second, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration())
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
				Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: activityTypeName},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	env.NoError(err)

	// make sure activities are unpaused
	env.EventuallyWithT(func(t *assert.CollectT) {
		description, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, 10*time.Second, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration())
		require.False(t, description.PendingActivities[0].Paused)

		description, err = env.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, 10*time.Second, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration())
		require.False(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	// let both of the activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	var out string
	err = workflowRun1.Get(ctx, &out)
	env.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	env.NoError(err)
}

func (s *ActivityAPIBatchUpdateOptionsSuite) TestActivityBatchUpdateOptionsFailed() {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.FrontendMaxConcurrentBatchOperationPerNamespace, testcore.ClientSuiteLimit))

	// neither activity type nor "match all" is provided
	_, err := env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	env.Error(err)
	env.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	env.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// neither activity type nor "match all" is provided
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{
				Activity: &batchpb.BatchOperationUpdateActivityOptions_Type{Type: ""},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	env.Error(err)
	env.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	env.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// cannot set activity options and restore original
	_, err = env.SdkClient().WorkflowService().StartBatchOperation(env.Context(), &workflowservice.StartBatchOperationRequest{
		Namespace: env.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{
				Activity: &batchpb.BatchOperationUpdateActivityOptions_Type{Type: "activity-type"},
				ActivityOptions: &activitypb.ActivityOptions{
					ScheduleToCloseTimeout: durationpb.New(1 * time.Second),
				},
				RestoreOriginal: true,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	env.Error(err)
	env.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	env.ErrorAs(err, new(*serviceerror.InvalidArgument))
}
