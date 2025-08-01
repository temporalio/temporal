package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/temporalio/sqlparser"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type ActivityApiBatchResetClientTestSuite struct {
	testcore.FunctionalTestBase
}

func TestActivityApiBatchResetClientTestSuite(t *testing.T) {
	s := new(ActivityApiBatchResetClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiBatchResetClientTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiBatchResetClientTestSuite) TestActivityBatchReset_Success() {
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
	resetCause := fmt.Sprintf("%s = %s", searchattribute.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: true,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, description.PendingActivities[0].Attempt, int32(1))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, description.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiBatchResetClientTestSuite) TestActivityBatchReset_Success_Protobuf() {
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
	resetCause := fmt.Sprintf("%s = %s", searchattribute.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: true,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, description.PendingActivities[0].Attempt, int32(1))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.True(t, description.PendingActivities[0].Paused)
		require.Equal(t, description.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiBatchResetClientTestSuite) TestActivityBatchReset_DontResetAttempts() {
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
	resetCause := fmt.Sprintf("%s = %s", searchattribute.TemporalPauseInfo, escapedSearchValue)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, resetCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		require.NoError(t, err)
		require.NotNil(t, listResp)
		require.Len(t, listResp.GetExecutions(), 2)
		require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))
	}, 5*time.Second, 500*time.Millisecond)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:      &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused:    false,
				ResetAttempts: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are restarted and still paused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotEqual(t, description.PendingActivities[0].Attempt, int32(1))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		require.NoError(t, err)
		require.Len(t, description.PendingActivities, 1)
		require.NotEqual(t, description.PendingActivities[0].Attempt, int32(1))
	}, 5*time.Second, 100*time.Millisecond)

	// let activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	// reset the activities in both workflows with batch reset
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity:   &batchpb.BatchOperationResetActivities_Type{Type: activityTypeName},
				KeepPaused: false,
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiBatchResetClientTestSuite) TestActivityBatchReset_Failed() {
	// neither activity type not "match all" is provided
	_, err := s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))

	// neither activity type not "match all" is provided
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_ResetActivitiesOperation{
			ResetActivitiesOperation: &batchpb.BatchOperationResetActivities{
				Activity: &batchpb.BatchOperationResetActivities_Type{Type: ""},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.Error(err)
	s.Equal(codes.InvalidArgument, serviceerror.ToStatus(err).Code())
	s.ErrorAs(err, new(*serviceerror.InvalidArgument))
}
