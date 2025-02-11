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
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type invokerExecutorsSuite struct {
	suite.Suite
	controller *gomock.Controller

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient  *historyservicemock.MockHistoryServiceClient

	env           *fakeEnv
	registry      *hsm.Registry
	backend       *hsmtest.NodeBackend
	rootNode      *hsm.Node
	schedulerNode *hsm.Node
	invokerNode   *hsm.Node
}

func TestInvokerExecutorsSuite(t *testing.T) {
	suite.Run(t, &invokerExecutorsSuite{})
}

func (e *invokerExecutorsSuite) SetupTest() {
	// Set up testing HSM environment.
	e.controller = gomock.NewController(e.T())
	e.env = newFakeEnv()
	e.registry = newRegistry(e.T())
	e.backend = &hsmtest.NodeBackend{}

	// Set up minimal scheduler tree.
	e.rootNode = newRoot(e.T(), e.registry, e.backend)
	e.schedulerNode = newSchedulerTree(e.T(), e.registry, e.rootNode, defaultSchedule(), nil)
	invokerNode, err := e.schedulerNode.Child([]hsm.Key{scheduler.InvokerMachineKey})
	require.NoError(e.T(), err)
	e.invokerNode = invokerNode
	e.env.node = invokerNode

	// Set up service mocks.
	e.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(e.controller)
	e.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(e.controller)

	// Register the task executor with mocks injected.
	require.NoError(e.T(), scheduler.RegisterInvokerExecutors(e.registry, scheduler.InvokerTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     log.NewTestLogger(),
		HistoryClient:  e.mockHistoryClient,
		FrontendClient: e.mockFrontendClient,
	}))
}

func (e *invokerExecutorsSuite) TearDownTest() {
	e.controller.Finish()
}

func (e *invokerExecutorsSuite) runExecuteTask() {
	err := e.registry.ExecuteImmediateTask(
		context.Background(),
		e.env,
		hsm.Ref{
			WorkflowKey:     definition.NewWorkflowKey("ns-id", "wf-id", "run-id"),
			StateMachineRef: &persistencespb.StateMachineRef{},
		},
		scheduler.ExecuteTask{})
	require.NoError(e.T(), err)
}

func (e *invokerExecutorsSuite) runProcessBufferTask() {
	err := e.registry.ExecuteTimerTask(e.env, e.invokerNode, scheduler.ProcessBufferTask{})
	require.NoError(e.T(), err)
}

// Execute success case.
func (e *invokerExecutorsSuite) TestExecuteTask_Basic() {
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		},
	}

	// Expect both buffered starts to result in workflow executions.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	e.runTestCase(&testCase{
		TaskType:                 scheduler.TaskTypeExecute,
		InitialBufferedStarts:    bufferedStarts,
		InitialState:             enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 2,
		ExpectedActionCount:      2,
		ExpectedState:            enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
	})
}

// ProcessBuffer attempts all buffered starts.
func (e *invokerExecutorsSuite) TestProcessBufferTask_AllowAll() {
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
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
			Manual:        false,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		},
	}

	e.runTestCase(&testCase{
		TaskType:               scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:  bufferedStarts,
		InitialState:           enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		ExpectedBufferedStarts: 3,
		ExpectedOverlapSkipped: 0,
		ExpectedState:          enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeExecute: 1,
		},
		Validate: func(t *testing.T, i scheduler.Invoker) {
			require.Equal(t, 3, len(util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			})))
		},
	})
}

// ProcessBuffer defers a start (from overlap policy) by placing it into
// NewBuffer.
func (e *invokerExecutorsSuite) TestProcessBufferTask_BufferOne() {
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
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
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req3",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE,
		},
	}

	e.runTestCase(&testCase{
		TaskType:               scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:  bufferedStarts,
		InitialState:           enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		ExpectedBufferedStarts: 2,
		ExpectedOverlapSkipped: 1,
		ExpectedState:          enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeExecute: 1,
		},
		Validate: func(t *testing.T, i scheduler.Invoker) {
			require.Equal(t, 1, len(util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
				return start.Attempt > 0
			})))
		},
	})
}

// Execute is scheduled with an empty buffer.
func (e *invokerExecutorsSuite) TestExecuteTask_Empty() {
	e.runTestCase(&testCase{
		TaskType:              scheduler.TaskTypeExecute,
		InitialBufferedStarts: nil,
		InitialState:          enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedState:         enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks:         map[string]int{},
	})
}

// ProcessBuffer is scheduled with an empty buffer.
func (e *invokerExecutorsSuite) TestProcessBufferTask_Empty() {
	e.runTestCase(&testCase{
		TaskType:              scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts: nil,
		InitialState:          enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		ExpectedState:         enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks:         map[string]int{},
	})
}

// ProcessBuffer is scheduled with a buffer of starts all backing off. No execute
// task should be scheduled.
func (e *invokerExecutorsSuite) TestProcessBufferTask_BackingOff() {
	startTime := timestamppb.New(e.env.Now())
	backoffTime := startTime.AsTime().Add(30 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "req2",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       3,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	e.runTestCase(&testCase{
		TaskType:               scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:  bufferedStarts,
		InitialState:           enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		ExpectedState:          enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedBufferedStarts: 2,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
		Validate: func(t *testing.T, i scheduler.Invoker) {
			// The ProcessBuffer task should have a backoff deadline.
			tasks := e.opLogTaskMap()
			bufferTasks := tasks[scheduler.TaskTypeProcessBuffer]
			require.Equal(e.T(), backoffTime, bufferTasks[0].Deadline())
		},
	})
}

// ProcessBuffer is scheduled with a start that was backing off, but ready to
// retry. Execute should be scheduled.
func (e *invokerExecutorsSuite) TestProcessBufferTask_BackingOffReady() {
	startTime := timestamppb.New(e.env.Now())
	backoffTime := e.env.Now().Add(-1 * time.Minute)
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req1",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
			BackoffTime:   timestamppb.New(backoffTime),
		},
	}

	e.runTestCase(&testCase{
		TaskType:               scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:  bufferedStarts,
		InitialState:           enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		ExpectedState:          enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedBufferedStarts: 1,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
			scheduler.TaskTypeExecute:       1,
		},
	})
}

// A buffered start is rate limited.
func (e *invokerExecutorsSuite) TestExecuteTask_RateLimited() {
	// TODO - add this test once we have a rate limiter to mock
	// Same as RetryableFailure, but uses the rate limiter's delay hint to punt the
	// start.
}

// // A buffered start fails with a retryable error.
func (e *invokerExecutorsSuite) TestExecuteTask_RetryableFailure() {
	// Set up the Invoker's buffer with a two starts. One will succeed immediately,
	// one will fail.
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "fail",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "pass",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
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

	e.runTestCase(&testCase{
		TaskType:                 scheduler.TaskTypeExecute,
		InitialBufferedStarts:    bufferedStarts,
		InitialState:             enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
		ExpectedOverlapSkipped:   0,
		ExpectedState:            enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
		Validate: func(t *testing.T, i scheduler.Invoker) {
			// The failed start should have had a backoff applied.
			failedStart := i.BufferedStarts[0]
			backoffTime := failedStart.BackoffTime.AsTime()
			require.True(e.T(), backoffTime.After(e.env.Now()))
			require.Equal(e.T(), int64(2), failedStart.Attempt)

			// The ProcessBuffer task should have a backoff deadline.
			tasks := e.opLogTaskMap()
			bufferTasks := tasks[scheduler.TaskTypeProcessBuffer]
			require.Equal(e.T(), backoffTime, bufferTasks[0].Deadline())
		},
	})
}

// A buffered start fails when a duplicate workflow has already been started.
func (e *invokerExecutorsSuite) TestExecuteTask_AlreadyStarted() {
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		},
	}

	// Fail with WorkflowExecutionAlreadyStarted.
	e.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""))

	e.runTestCase(&testCase{
		TaskType:                 scheduler.TaskTypeExecute,
		InitialBufferedStarts:    bufferedStarts,
		InitialState:             enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
		ExpectedState:            enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
	})
}

// A buffered start fails from having exceeded its maximum retry limit.
func (e *invokerExecutorsSuite) TestExecuteTask_ExceedsMaxAttempts() {
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
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

	e.runTestCase(&testCase{
		TaskType:                 scheduler.TaskTypeExecute,
		InitialBufferedStarts:    bufferedStarts,
		InitialState:             enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
		ExpectedState:            enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
	})
}

// A buffered start with an overlap policy to terminate other workflows is processed.
func (e *invokerExecutorsSuite) TestProcessBufferTask_NeedsTerminate() {
	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will terminate existing workflows.
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER,
		},
	}

	e.runTestCase(&testCase{
		TaskType:                scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		InitialState:            enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		//
		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:     1,
		ExpectedRunningWorkflows:   1,
		ExpectedTerminateWorkflows: 1,
		ExpectedActionCount:        0,
		ExpectedState:              enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeExecute: 1,
		},
	})
}

// A buffered start with an overlap policy to cancel other workflows is processed.
func (e *invokerExecutorsSuite) TestProcessBufferTask_NeedsCancel() {
	// Add a running workflow to the Scheduler.
	initialRunningWorkflows := []*commonpb.WorkflowExecution{{
		WorkflowId: "existing-wf",
		RunId:      "existing-run",
	}}

	// Set up the BufferedStart with a policy that will cancel existing workflows.
	startTime := timestamppb.New(e.env.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "new-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER,
		},
	}

	e.runTestCase(&testCase{
		TaskType:                scheduler.TaskTypeProcessBuffer,
		InitialBufferedStarts:   bufferedStarts,
		InitialRunningWorkflows: initialRunningWorkflows,
		InitialState:            enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,

		// Buffer should still contain the buffered start. The existing workflow will still
		// remain in RunningWorkflows as well, since it is the Watcher's job to remove it
		// after termination/cancelation takes effect.
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedCancelWorkflows:  1,
		ExpectedActionCount:      0,
		ExpectedState:            enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeExecute: 1,
		},
	})
}

// An execute task runs with cancels/terminations queued, which fail to execute.
func (e *invokerExecutorsSuite) TestExecuteTask_CancelTerminateFailure() {
	cancelWorkflows := []*commonpb.WorkflowExecution{
		{
			WorkflowId: "wf",
			RunId:      "run1",
		},
	}
	terminateWorkflows := []*commonpb.WorkflowExecution{
		{
			WorkflowId: "wf",
			RunId:      "run2",
		},
	}

	// Fail both service calls.
	e.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))
	e.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))

	// Terminate and Cancel are both attempted only once. Regardless of the service
	// call's outcome, they should have been removed from the Invoker's queue.
	e.runTestCase(&testCase{
		TaskType:                   scheduler.TaskTypeExecute,
		InitialBufferedStarts:      nil,
		InitialCancelWorkflows:     cancelWorkflows,
		InitialTerminateWorkflows:  terminateWorkflows,
		InitialState:               enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedState:              enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedBufferedStarts:     0,
		ExpectedRunningWorkflows:   0,
		ExpectedActionCount:        0,
		ExpectedCancelWorkflows:    0,
		ExpectedTerminateWorkflows: 0,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
	})
}

// An Execute task runs with cancels/terminations queued, resulting in success.
func (e *invokerExecutorsSuite) TestExecuteTask_CancelTerminateSucceed() {
	cancelWorkflows := []*commonpb.WorkflowExecution{
		{
			WorkflowId: "wf",
			RunId:      "run1",
		},
	}
	terminateWorkflows := []*commonpb.WorkflowExecution{
		{
			WorkflowId: "wf",
			RunId:      "run2",
		},
	}

	// Succeed both service calls.
	e.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)
	e.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)

	e.runTestCase(&testCase{
		TaskType:                   scheduler.TaskTypeExecute,
		InitialBufferedStarts:      nil,
		InitialCancelWorkflows:     cancelWorkflows,
		InitialTerminateWorkflows:  terminateWorkflows,
		InitialState:               enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		ExpectedState:              enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
		ExpectedBufferedStarts:     0,
		ExpectedRunningWorkflows:   0,
		ExpectedActionCount:        0,
		ExpectedCancelWorkflows:    0,
		ExpectedTerminateWorkflows: 0,
		ExpectedTasks: map[string]int{
			scheduler.TaskTypeProcessBuffer: 1,
		},
	})
}

type testCase struct {
	TaskType string

	InitialBufferedStarts     []*schedulespb.BufferedStart
	InitialCancelWorkflows    []*commonpb.WorkflowExecution
	InitialTerminateWorkflows []*commonpb.WorkflowExecution
	InitialRunningWorkflows   []*commonpb.WorkflowExecution
	InitialState              enumsspb.SchedulerInvokerState

	ExpectedBufferedStarts     int
	ExpectedRunningWorkflows   int
	ExpectedTerminateWorkflows int
	ExpectedCancelWorkflows    int
	ExpectedActionCount        int64
	ExpectedOverlapSkipped     int64
	ExpectedState              enumsspb.SchedulerInvokerState
	ExpectedTasks              map[string]int // task type -> expected count

	Validate func(*testing.T, scheduler.Invoker) // Called after all other validations pass for additional assertions.
}

func (e *invokerExecutorsSuite) runTestCase(c *testCase) {
	t := e.T()

	schedulerSm, err := hsm.MachineData[scheduler.Scheduler](e.schedulerNode)
	require.NoError(t, err)
	schedulerSm.Info.RunningWorkflows = c.InitialRunningWorkflows

	invoker, err := hsm.MachineData[scheduler.Invoker](e.invokerNode)
	require.NoError(t, err)
	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows
	invoker.SetState(c.InitialState)
	invoker.LastProcessedTime = timestamppb.New(e.env.Now())

	switch c.TaskType {
	case scheduler.TaskTypeProcessBuffer:
		e.runProcessBufferTask()
	case scheduler.TaskTypeExecute:
		e.runExecuteTask()
	default:
		t.FailNow()
	}

	require.Equal(t, c.ExpectedBufferedStarts, len(invoker.BufferedStarts))
	require.Equal(t, c.ExpectedRunningWorkflows, len(schedulerSm.Info.RunningWorkflows))
	require.Equal(t, c.ExpectedTerminateWorkflows, len(invoker.TerminateWorkflows))
	require.Equal(t, c.ExpectedCancelWorkflows, len(invoker.CancelWorkflows))
	require.Equal(t, c.ExpectedActionCount, schedulerSm.Info.ActionCount)
	require.Equal(t, c.ExpectedOverlapSkipped, schedulerSm.Info.OverlapSkipped)
	require.Equal(t, c.ExpectedState, invoker.State())

	tasks := e.opLogTaskMap()
	for taskType, count := range c.ExpectedTasks {
		actualCount := len(tasks[taskType])
		require.Equal(t, count, actualCount, "expected %d %s tasks, had %d", count, taskType, actualCount)
	}
	require.Equal(t, len(c.ExpectedTasks), len(tasks))

	if c.Validate != nil {
		c.Validate(t, invoker)
	}
}

// opLogTaskMap returns a map from task type -> []hsm.Task{}.
func (e *invokerExecutorsSuite) opLogTaskMap() map[string][]hsm.Task {
	result := make(map[string][]hsm.Task)
	tasks, err := opLogTasks(e.rootNode)
	require.NoError(e.T(), err)

	for _, task := range tasks {
		key := task.Type()
		result[key] = append(result[key], task)
	}

	return result
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
