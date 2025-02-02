// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package scheduler_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type executorExecutorsSuite struct {
	suite.Suite
	controller *gomock.Controller

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient  *historyservicemock.MockHistoryServiceClient

	env           *fakeEnv
	registry      *hsm.Registry
	backend       *hsmtest.NodeBackend
	rootNode      *hsm.Node
	schedulerNode *hsm.Node
	executorNode  *hsm.Node
}

func TestExecutorExecutorsSuite(t *testing.T) {
	suite.Run(t, &executorExecutorsSuite{})
}

func (e *executorExecutorsSuite) SetupTest() {
	// Set up testing HSM environment.
	e.controller = gomock.NewController(e.T())
	e.env = newFakeEnv()
	e.registry = newRegistry(e.T())
	e.backend = &hsmtest.NodeBackend{}

	// Set up minimal scheduler tree.
	e.rootNode = newRoot(e.T(), e.registry, e.backend)
	e.schedulerNode = newSchedulerTree(e.T(), e.registry, e.rootNode, defaultSchedule(), nil)
	executorNode, err := e.schedulerNode.Child([]hsm.Key{scheduler.ExecutorMachineKey})
	require.NoError(e.T(), err)
	e.executorNode = executorNode

	// Set up service mocks.
	e.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(e.controller)
	e.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(e.controller)

	// Register the task executor with mocks injected.
	require.NoError(e.T(), scheduler.RegisterExecutorExecutors(e.registry, scheduler.ExecutorTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewTestLogger(),
		HistoryClient:  e.mockHistoryClient,
		FrontendClient: e.mockFrontendClient,
	}))
}

func (e *executorExecutorsSuite) TearDownTest() {
	e.controller.Finish()
}

// Success case.
func (e *executorExecutorsSuite) TestExecuteTask_Basic() {
	// Set up the Executor's buffer with a few starts.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	// Expect both buffered starts to result in workflow executions.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should be empty, scheduler metadata should be updated.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(executor.BufferedStarts))
	require.Equal(e.T(), 2, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), 2, len(schedulerSm.Info.RecentActions))
	require.Equal(e.T(), int64(2), schedulerSm.Info.ActionCount)

	// Executor should have transitioned to waiting with no additional tasks scheduled.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// ProcessBuffer defers a start (from overlap policy) by placing it into
// NewBuffer.
func (e *executorExecutorsSuite) TestExecuteTask_BufferOne() {
	// Set up the Executor's buffer with a few starts.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	// Expect a single buffered starts to result in a workflow execution.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should have one remaining start, scheduler metadata should be updated.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 1, len(executor.BufferedStarts))
	require.Equal(e.T(), 1, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), 1, len(schedulerSm.Info.RecentActions))
	require.Equal(e.T(), int64(1), schedulerSm.Info.ActionCount)

	// Executor should have moved to waiting with no further tasks scheduled. Watcher
	// will wake up Executor when a workflow closes.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// Execute is scheduled with an empty buffer.
func (e *executorExecutorsSuite) TestExecuteTask_Empty() {
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)

	// Leave the Executor's buffer empty, then run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Executor should have stayed in waiting with no additional tasks scheduled.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// A buffered start is rate limited.
func (e *executorExecutorsSuite) TestExecuteTask_RateLimited() {
	// TODO - add this test once we have a rate limiter to mock
	// Same as RetryableFailure, but uses the rate limiter's delay hint to punt the
	// start.
}

// A buffered start fails with a retryable error.
func (e *executorExecutorsSuite) TestExecuteTask_RetryableFailure() {
	// Set up the Executor's buffer with a two starts. One will succeed immediately,
	// one will fail.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "fail",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "pass",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	// Fail the first start, and succeed the second.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIdMatches("fail")).
		Times(1).
		Return(nil, serviceerror.NewDeadlineExceeded("deadline exceeded"))
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIdMatches("pass")).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should have the failed workflow, the other should have kicked off.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 1, len(executor.BufferedStarts))
	require.Equal(e.T(), 1, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), int64(1), schedulerSm.Info.ActionCount)

	// The failed start should have had a backoff applied.
	failedStart := executor.BufferedStarts[0]
	require.True(e.T(), failedStart.BackoffTime.AsTime().After(e.env.Now()))
	require.Equal(e.T(), int64(1), failedStart.Attempt)

	// Another Execute task should have been generated with a delay.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 1, len(tasks))
	task := tasks[0]
	require.Equal(e.T(), failedStart.BackoffTime.AsTime(), task.Deadline())
	require.Equal(e.T(), scheduler.TaskTypeExecute, task.Type())
}

// A buffered start fails when a duplicate workflow has already been started.
func (e *executorExecutorsSuite) TestExecuteTask_AlreadyStarted() {
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	// Fail with WorkflowExecutionAlreadyStarted.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""))

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should have dropped the non-retryable workflow.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(executor.BufferedStarts))
	require.Equal(e.T(), 0, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), int64(0), schedulerSm.Info.ActionCount)

	// The Executor should have moved back to waiting without additional tasks.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// A buffered start fails from having exceeded its maximum retry limit.
func (e *executorExecutorsSuite) TestExecuteTask_ExceedsMaxAttempts() {
	// Set up the buffer with a start at the retry limit.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       scheduler.ExecutorMaxStartAttempts,
		},
	}

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should have dropped the continually failing start.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(executor.BufferedStarts))
	require.Equal(e.T(), 0, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), int64(0), schedulerSm.Info.ActionCount)

	// The Executor should have moved back to waiting without additional tasks.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// A buffered start with an overlap policy to terminate other workflows is executed.
func (e *executorExecutorsSuite) TestExecuteTask_NeedsTerminate() {
	// Set up the BufferedStart with a policy that will terminate existing workflows.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		},
	}

	// Add a running workflow to the Scheduler.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	existingExecution := &commonpb.WorkflowExecution{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}
	schedulerSm.Info.RunningWorkflows = []*commonpb.WorkflowExecution{existingExecution}

	// Expect the running workflow to be terminated.
	e.mockHistoryClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(&historyservice.TerminateWorkflowExecutionResponse{}, nil)

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should still contain the buffered start. The existing workflow will still
	// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
	// after termination/cancelation takes effect.
	require.Equal(e.T(), 1, len(executor.BufferedStarts))
	require.Equal(e.T(), 1, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), int64(0), schedulerSm.Info.ActionCount)

	// The Executor should have moved back to waiting without additional tasks.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

// A buffered start with an overlap policy to cancel other workflows is executed.
func (e *executorExecutorsSuite) TestExecuteTask_NeedsCancel() {
	// Set up the BufferedStart with a policy that will cancel existing workflows.
	executor, err := hsm.MachineData[scheduler.Executor](e.executorNode)
	require.NoError(e.T(), err)
	startTime := timestamppb.New(e.env.Now())
	executor.BufferedStarts = []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		},
	}

	// Add a running workflow to the Scheduler.
	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(e.T(), err)
	existingExecution := &commonpb.WorkflowExecution{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}
	schedulerSm.Info.RunningWorkflows = []*commonpb.WorkflowExecution{existingExecution}

	// Expect the running workflow to be canceled.
	e.mockHistoryClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(&historyservice.RequestCancelWorkflowExecutionResponse{}, nil)

	// Run an Execute task.
	err = e.registry.ExecuteTimerTask(e.env, e.executorNode, scheduler.ExecuteTask{})
	require.NoError(e.T(), err)

	// Buffer should still contain the buffered start. The existing workflow will still
	// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
	// after termination/cancelation takes effect.
	require.Equal(e.T(), 1, len(executor.BufferedStarts))
	require.Equal(e.T(), 1, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(e.T(), int64(0), schedulerSm.Info.ActionCount)

	// The Executor should have moved back to waiting without additional tasks.
	require.Equal(e.T(), enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING, executor.State())
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)
	require.Equal(e.T(), 0, len(tasks))
}

type startWorkflowExecutionRequestIdMatcher struct {
	RequestId string
}

var _ gomock.Matcher = &startWorkflowExecutionRequestIdMatcher{}

func startWorkflowExecutionRequestIdMatches(requestId string) *startWorkflowExecutionRequestIdMatcher {
	return &startWorkflowExecutionRequestIdMatcher{requestId}
}

func (s *startWorkflowExecutionRequestIdMatcher) String() string {
	return fmt.Sprintf("StartWorkflowExecutionRequest{RequestId: \"%s\"}", s.RequestId)
}

func (s *startWorkflowExecutionRequestIdMatcher) Matches(x interface{}) bool {
	req, ok := x.(*workflowservice.StartWorkflowExecutionRequest)
	return ok && req.RequestId == s.RequestId
}
