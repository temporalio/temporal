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
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type ActivityApiBatchUnpauseClientTestSuite struct {
	testcore.FunctionalTestSdkSuite
	tv                     *testvars.TestVars
	initialRetryInterval   time.Duration
	scheduleToCloseTimeout time.Duration
	startToCloseTimeout    time.Duration

	activityRetryPolicy *temporal.RetryPolicy
}

func TestActivityApiBatchUnpauseClientTestSuite(t *testing.T) {
	s := new(ActivityApiBatchUnpauseClientTestSuite)
	suite.Run(t, s)
}

func (s *ActivityApiBatchUnpauseClientTestSuite) SetupTest() {
	s.FunctionalTestSdkSuite.SetupTest()

	s.tv = testvars.New(s.T()).WithTaskQueue(s.TaskQueue()).WithNamespaceName(s.Namespace())

	s.initialRetryInterval = 1 * time.Second
	s.scheduleToCloseTimeout = 30 * time.Minute
	s.startToCloseTimeout = 15 * time.Minute

	s.activityRetryPolicy = &temporal.RetryPolicy{
		InitialInterval:    s.initialRetryInterval,
		BackoffCoefficient: 1,
	}
}

func (s *ActivityApiBatchUnpauseClientTestSuite) makeWorkflowFunc(activityFunction ActivityFunctions) WorkflowFunction {
	return func(ctx workflow.Context) error {

		var ret string
		err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			ActivityID:             "activity-id",
			DisableEagerExecution:  true,
			StartToCloseTimeout:    s.startToCloseTimeout,
			ScheduleToCloseTimeout: s.scheduleToCloseTimeout,
			RetryPolicy:            s.activityRetryPolicy,
		}), activityFunction).Get(ctx, &ret)
		return err
	}
}

func (s *ActivityApiBatchUnpauseClientTestSuite) createWorkflow(ctx context.Context, workflowFn WorkflowFunction) sdkclient.WorkflowRun {
	workflowOptions1 := sdkclient.StartWorkflowOptions{
		ID:        testcore.RandomizeStr("wf_id-" + s.T().Name()),
		TaskQueue: s.TaskQueue(),
	}
	workflowRun, err := s.SdkClient().ExecuteWorkflow(ctx, workflowOptions1, workflowFn)
	s.NoError(err)
	s.NotNil(workflowRun)

	return workflowRun
}

func (s *ActivityApiBatchUnpauseClientTestSuite) TestActivityBatchUnpause_Acceptance() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var startedActivityCount atomic.Int32
	var letActivitySucceed atomic.Bool

	activityFunction := func() (string, error) {
		startedActivityCount.Add(1)
		if letActivitySucceed.Load() == false {
			activityErr := errors.New("bad-luck-please-retry")
			return "", activityErr
		}
		return "done!", nil
	}

	workflowFn := s.makeWorkflowFunc(activityFunction)

	s.Worker().RegisterWorkflow(workflowFn)
	s.Worker().RegisterActivity(activityFunction)

	workflowRun1 := s.createWorkflow(ctx, workflowFn)
	workflowRun2 := s.createWorkflow(ctx, workflowFn)

	// wait for activity to start in both workflows
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun1.GetID(), workflowRun1.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
		}
		assert.Greater(t, startedActivityCount.Load(), int32(0))

		description, err = s.SdkClient().DescribeWorkflowExecution(ctx, workflowRun2.GetID(), workflowRun2.GetRunID())
		assert.NoError(t, err)
		if description.GetPendingActivities() != nil {
			assert.Len(t, description.PendingActivities, 1)
		}
		assert.Greater(t, startedActivityCount.Load(), int32(0))
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
			assert.Len(t, description.PendingActivities, 1)
			assert.True(t, description.PendingActivities[0].Paused)
		}
	}, 5*time.Second, 100*time.Millisecond)

	// yes, both workflow type and activity type are "func1". There is no way to specify them.
	typeName := "func1"
	// Make sure the activity is in visibility
	var listResp *workflowservice.ListWorkflowExecutionsResponse
	//unpauseCause := fmt.Sprintf("%s = 'property:activityType=%s'", searchattribute.TemporalPauseInfo, typeName)
	//query := fmt.Sprintf("(WorkflowType='%s' AND %s)", typeName, unpauseCause)
	query := fmt.Sprintf("(WorkflowType='%s')", typeName)
	s.Logger.Info(fmt.Sprintf("QQQQQQQ %s", query))

	scanRequest := &workflowservice.ScanWorkflowExecutionsRequest{
		Namespace: s.Namespace().String(),
		PageSize:  int32(10),
		Query:     fmt.Sprintf(`WorkflowType = '%s' and ExecutionStatus = 'Running'`, typeName),
	}

	s.EventuallyWithT(func(t *assert.CollectT) {
		scanResponse, err := s.FrontendClient().ScanWorkflowExecutions(testcore.NewContext(), scanRequest)
		assert.NoError(t, err)
		assert.NotNil(t, scanResponse)
		if scanResponse != nil {
			s.Logger.Info(fmt.Sprintf("QQQQQQQ ScanWorkflowExecutions %v", len(scanResponse.Executions)))
			assert.Len(t, scanResponse.Executions, 2)
		}

	}, 5*time.Second, 500*time.Millisecond)

	s.EventuallyWithT(func(t *assert.CollectT) {
		listResp, err = s.FrontendClient().ListWorkflowExecutions(ctx, &workflowservice.ListWorkflowExecutionsRequest{
			Namespace: s.Namespace().String(),
			PageSize:  10,
			Query:     query,
		})
		assert.NoError(t, err)
		assert.NotNil(t, listResp)
		if listResp != nil {
			s.Logger.Info(fmt.Sprintf("QQQQQQQ %v", len(listResp.Executions)))
			assert.Len(t, listResp.Executions, 2)
		}
	}, 5*time.Second, 500*time.Millisecond)

	// unpause the activities in both workflows with batch unpause
	_, err = s.SdkClient().WorkflowService().StartBatchOperation(context.Background(), &workflowservice.StartBatchOperationRequest{
		Namespace: s.Namespace().String(),
		Operation: &workflowservice.StartBatchOperationRequest_UnpauseActivitiesOperation{
			UnpauseActivitiesOperation: &batchpb.BatchOperationUnpauseActivities{
				Activity: &batchpb.BatchOperationUnpauseActivities_Type{Type: typeName},
			},
		},
		VisibilityQuery: fmt.Sprintf("WorkflowType='%s'", typeName),
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
	letActivitySucceed.Store(true)

	var out string
	err = workflowRun1.Get(ctx, &out)
	s.NoError(err)

	err = workflowRun2.Get(ctx, &out)
	s.NoError(err)
}
