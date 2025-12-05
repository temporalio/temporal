package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	activitypb "go.temporal.io/api/activity/v1"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

type ActivityApiBatchUpdateOptionsClientTestSuite struct {
	testcore.FunctionalTestBase
}

func TestActivityApiBatchUpdateOptionsClientTestSuite(t *testing.T) {
	s := new(ActivityApiBatchUpdateOptionsClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiBatchUpdateOptionsClientTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiBatchUpdateOptionsClientTestSuite) TestActivityBatchUpdateOptions_Success() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	internalWorkflow := newInternalWorkflow()

	s.Worker().RegisterWorkflow(internalWorkflow.WorkflowFunc)
	s.Worker().RegisterActivity(internalWorkflow.ActivityFunc)

	workflowRun1 := s.createWorkflow(ctx, internalWorkflow.WorkflowFunc)
	workflowRun2 := s.createWorkflow(ctx, internalWorkflow.WorkflowFunc)

	// wait for activity to start in both workflows
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.GetPendingActivities(), 1)
		require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))
	}, 5*time.Second, 100*time.Millisecond)

	// pause activities in both workflows
	pauseRequest := &workflowservice.PauseActivityRequest{
		Namespace: s.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{},
		Activity:  &workflowservice.PauseActivityRequest_Id{Id: "activity-id"},
	}
	pauseRequest.Execution.WorkflowId = workflowRun1.GetID()
	resp, err := s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
	resp, err = s.FrontendClient().PauseActivity(ctx, pauseRequest)
	s.NoError(err)
	s.NotNil(resp)

	// wait for activities to be paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
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

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
	}, 5*time.Second, 500*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
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
	s.NoError(err)

	// make sure activities are unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration(), 10*time.Second)
		require.True(t, description.PendingActivities[0].Paused)

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration(), 10*time.Second)
		require.True(t, description.PendingActivities[0].Paused)
	}, 5*time.Second, 100*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
				Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: activityTypeName},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration(), 10*time.Second)
		require.Equal(t, description.PendingActivities[0].Paused, false)

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.Equal(t, description.PendingActivities[0].ActivityOptions.ScheduleToCloseTimeout.AsDuration(), 10*time.Second)
		require.Equal(t, description.PendingActivities[0].Paused, false)
	}, 5*time.Second, 100*time.Millisecond)

	// let both of the activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiBatchUpdateOptionsClientTestSuite) TestActivityBatchUpdateOptions_Failed() {
	// neither activity type nor "match all" is provided
	_, err := s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// neither activity type nor "match all" is provided
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UpdateActivityOptionsOperation{
			UpdateActivityOptionsOperation: &batchpb.BatchOperationUpdateActivityOptions{
				Activity: &batchpb.BatchOperationUpdateActivityOptions_Type{Type: ""},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.NewString(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// cannot set activity options and restore original
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
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
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))
}
