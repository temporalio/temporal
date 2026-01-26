package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/temporalio/sqlparser"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/searchattribute/sadefs"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type internalTestWorkflow struct {
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy

	startedActivityCount atomic.Int32
	letActivitySucceed   atomic.Bool
}

func newInternalWorkflow() *internalTestWorkflow {
	wf := &internalTestWorkflow{
		initialRetryInterval:   1 * time.Second,
		scheduleToCloseTimeout: 30 * time.Minute,
		startToCloseTimeout:    15 * time.Minute,
	}
	wf.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    wf.initialRetryInterval,
		BackoffCoefficient: 1,
	}

	return wf
}

func (w *internalTestWorkflow) WorkflowFunc(ctx workflow.Context) error {
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ActivityID:             "activity-id",
		DisableEagerExecution:  true,
		StartToCloseTimeout:    w.startToCloseTimeout,
		ScheduleToCloseTimeout: w.scheduleToCloseTimeout,
		RetryPolicy:            w.activityRetryPolicy,
	}), w.ActivityFunc).Get(ctx, nil)
	return err
}

func (w *internalTestWorkflow) ActivityFunc() (string, error) {
	w.startedActivityCount.Add(1)
	if w.letActivitySucceed.Load() == false {
		activityErr := errors.New("bad-luck-please-retry")
		return "", activityErr
	}
	return "done!", nil
}

func TestActivityBatchUnpause(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		s := testcore.NewEnv(t)

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Set up SDK client and worker
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		taskQueue := s.Tv().TaskQueue().Name
		w := worker.New(sdkClient, taskQueue, worker.Options{})

		internalWorkflow := newInternalWorkflow()
		w.RegisterWorkflow(internalWorkflow.WorkflowFunc)
		w.RegisterActivity(internalWorkflow.ActivityFunc)
		require.NoError(t, w.Start())
		defer w.Stop()

		// Helper function to create workflow
		createWorkflow := func(workflowFn interface{}, suffix string) sdkclient.WorkflowRun {
			workflowOptions := sdkclient.StartWorkflowOptions{
				ID:        testcore.RandomizeStr("wf_id-" + t.Name() + "-" + suffix),
				TaskQueue: taskQueue,
			}
			workflowRun, err := sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
			require.NoError(t, err)
			require.NotNil(t, workflowRun)
			return workflowRun
		}

		workflowRun1 := createWorkflow(internalWorkflow.WorkflowFunc, "1")
		workflowRun2 := createWorkflow(internalWorkflow.WorkflowFunc, "2")

		// wait for activity to start in both workflows
		s.EventuallyWithT(func(t *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
			require.NoError(t, err)
			require.Len(t, description.GetPendingActivities(), 1)
			require.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))

			description, err = sdkClient.DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
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
		require.NoError(t, err)
		require.NotNil(t, resp)

		pauseRequest.Execution.WorkflowId = workflowRun2.GetID()
		resp, err = s.FrontendClient().PauseActivity(ctx, pauseRequest)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// wait for activities to be paused
		s.EventuallyWithT(func(t *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
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
		_, err = sdkClient.WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
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
		require.NoError(t, err)

		// make sure activities are unpaused
		s.EventuallyWithT(func(t *assert.CollectT) {
			description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
			require.NoError(t, err)
			require.Len(t, description.PendingActivities, 1)
			require.False(t, description.PendingActivities[0].Paused)
			description, err = sdkClient.DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
			require.NoError(t, err)
			require.Len(t, description.PendingActivities, 1)
			require.False(t, description.PendingActivities[0].Paused)
		}, 5*time.Second, 100*time.Millisecond)

		// let both of the activities succeed
		internalWorkflow.letActivitySucceed.Store(true)

		var out string
		err = workflowRun1.Get(ctx, &out)
		require.NoError(t, err)

		err = workflowRun2.Get(ctx, &out)
		require.NoError(t, err)
	})

	t.Run("Failed", func(t *testing.T) {
		s := testcore.NewEnv(t)

		// Set up SDK client
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:  s.FrontendGRPCAddress(),
			Namespace: s.Namespace().String(),
			Logger:    log.NewSdkLogger(s.Logger),
		})
		require.NoError(t, err)
		defer sdkClient.Close()

		// neither activity type not "match all" is provided
		_, err = sdkClient.WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
			Namespace: s.Namespace().String(),
			Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
				UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{},
			},
			VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
			JobId:           uuid.NewString(),
			Reason:          "test",
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))

		// neither activity type not "match all" is provided
		_, err = sdkClient.WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
			Namespace: s.Namespace().String(),
			Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
				UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
					Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: ""},
				},
			},
			VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", "WorkflowFunc"),
			JobId:           uuid.NewString(),
			Reason:          "test",
		})
		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, serviceerror.ToStatus(err).Code())
		require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
	})
}
