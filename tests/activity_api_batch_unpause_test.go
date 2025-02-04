// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	batchpb "go.temporal.io/api/batch/v1"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/grpc/codes"
)

type ActivityApiBatchUnpauseClientTestSuite struct {
	testcore.FunctionalTestSdkSuite
}

func TestActivityApiBatchUnpauseClientTestSuite(t *testing.T) {
	s := new(ActivityApiBatchUnpauseClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiBatchUnpauseClientTestSuite) SetupTest() {
	s.FunctionalTestSdkSuite.SetupTest()
}

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

func (s *ActivityApiBatchUnpauseClientTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiBatchUnpauseClientTestSuite) TestActivityBatchUnpause_Success() {
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
		assert.NoError(t, err)
		assert.Len(t, description.GetPendingActivities(), 1)
		assert.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		assert.NoError(t, err)
		assert.Len(t, description.GetPendingActivities(), 1)
		assert.Greater(t, internalWorkflow.startedActivityCount.Load(), int32(0))
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
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.GetPendingActivities(), 1)
			assert.True(t, description.PendingActivities[0].Paused)
		}
	}, 5*time.Second, 100*time.Millisecond)

	workflowTypeName := "WorkflowFunc"
	activityTypeName := "ActivityFunc"
	// Make sure the activity is in visibility
	var listResp *workflowservice.ListWorkflowExecutionsResponse
	unpauseCause := fmt.Sprintf("%s = 'property:activityType=%s'", searchattribute.TemporalPauseInfo, activityTypeName)
	query := fmt.Sprintf("(WorkflowType='%s' AND %s)", workflowTypeName, unpauseCause)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		assert.NoError(t, err)
		assert.NotNil(t, listResp)
		assert.Len(t, listResp.GetExecutions(), 2)
	}, 5*time.Second, 500*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
				Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: activityTypeName},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", workflowTypeName),
		JobId:           uuid.New(),
		Reason:          "test",
	})
	s.NoError(err)

	// make sure activities are unpaused
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.False(t, description.PendingActivities[0].Paused)
		}
		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
			assert.False(t, description.PendingActivities[0].Paused)
		}
	}, 5*time.Second, 100*time.Millisecond)

	// let both of the activities succeed
	internalWorkflow.letActivitySucceed.Store(true)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}

func (s *ActivityApiBatchUnpauseClientTestSuite) TestActivityBatchUnpause_Failed() {
	// neither activity type not "match all" is provided
	_, err := s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{},
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
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
				Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: ""},
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
