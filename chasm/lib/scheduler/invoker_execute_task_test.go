package scheduler_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type invokerExecuteTaskSuite struct {
	schedulerSuite
	executor *scheduler.InvokerExecuteTaskExecutor

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient  *historyservicemock.MockHistoryServiceClient
}

func TestInvokerExecuteTaskSuite(t *testing.T) {
	suite.Run(t, &invokerExecuteTaskSuite{})
}

func (s *invokerExecuteTaskSuite) SetupTest() {
	s.SetupSuite()

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
	s.mockHistoryClient = historyservicemock.NewMockHistoryServiceClient(s.controller)

	s.executor = scheduler.NewInvokerExecuteTaskExecutor(scheduler.InvokerTaskExecutorOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     s.logger,
		HistoryClient:  s.mockHistoryClient,
		FrontendClient: s.mockFrontendClient,
	})
}

type executeTestCase struct {
	InitialBufferedStarts     []*schedulespb.BufferedStart
	InitialCancelWorkflows    []*commonpb.WorkflowExecution
	InitialTerminateWorkflows []*commonpb.WorkflowExecution
	InitialRunningWorkflows   []*commonpb.WorkflowExecution

	ExpectedBufferedStarts      int
	ExpectedRunningWorkflows    int
	ExpectedTerminateWorkflows  int
	ExpectedCancelWorkflows     int
	ExpectedActionCount         int64
	ExpectedOverlapSkipped      int64
	ExpectedMissedCatchupWindow int64

	ValidateInvoker func(invoker *scheduler.Invoker)
}

// Execute success case.
func (s *invokerExecuteTaskSuite) TestExecuteTask_Basic() {
	startTime := timestamppb.New(s.timeSource.Now())
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
	s.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 2,
		ExpectedActionCount:      2,
	})
}

// Execute is scheduled with an empty buffer.
func (s *invokerExecuteTaskSuite) TestExecuteTask_Empty() {
	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts: nil,
	})
}

// A buffered start fails with a retryable error.
func (s *invokerExecuteTaskSuite) TestExecuteTask_RetryableFailure() {
	// Set up the Invoker's buffer with a two starts. One will succeed immediately,
	// one will fail.
	startTime := timestamppb.New(s.timeSource.Now())
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
	s.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("fail")).
		Times(1).
		Return(nil, serviceerror.NewDeadlineExceeded("deadline exceeded"))
	s.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("pass")).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
		ValidateInvoker: func(invoker *scheduler.Invoker) {
			// The failed start should have had a backoff applied.
			failedStart := invoker.BufferedStarts[0]
			backoffTime := failedStart.BackoffTime.AsTime()
			s.True(backoffTime.After(s.timeSource.Now()))
			s.Equal(int64(2), failedStart.Attempt)
		},
	})
}

// A buffered start fails when a duplicate workflow has already been started.
func (s *invokerExecuteTaskSuite) TestExecuteTask_AlreadyStarted() {
	startTime := timestamppb.New(s.timeSource.Now())
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
	s.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""))

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
	})
}

// A buffered start fails from having exceeded its maximum retry limit.
func (s *invokerExecuteTaskSuite) TestExecuteTask_ExceedsMaxAttempts() {
	startTime := timestamppb.New(s.timeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "req",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       scheduler.InvokerMaxStartAttempts,
		},
	}

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
	})
}

// An execute task runs with cancels/terminations queued, which fail to execute.
func (s *invokerExecuteTaskSuite) TestExecuteTask_CancelTerminateFailure() {
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
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))

	// Terminate and Cancel are both attempted only once. Regardless of the service
	// call's outcome, they should have been removed from the Invoker's queue.
	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:      nil,
		InitialCancelWorkflows:     cancelWorkflows,
		InitialTerminateWorkflows:  terminateWorkflows,
		ExpectedBufferedStarts:     0,
		ExpectedRunningWorkflows:   0,
		ExpectedActionCount:        0,
		ExpectedCancelWorkflows:    0,
		ExpectedTerminateWorkflows: 0,
	})
}

// An Execute task runs with cancels/terminations queued, resulting in success.
func (s *invokerExecuteTaskSuite) TestExecuteTask_CancelTerminateSucceed() {
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
	s.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)
	s.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:      nil,
		InitialCancelWorkflows:     cancelWorkflows,
		InitialTerminateWorkflows:  terminateWorkflows,
		ExpectedBufferedStarts:     0,
		ExpectedRunningWorkflows:   0,
		ExpectedActionCount:        0,
		ExpectedCancelWorkflows:    0,
		ExpectedTerminateWorkflows: 0,
	})
}

// Tests when the ExecuteTask should yield by completing and committing any
// completed work.
func (s *invokerExecuteTaskSuite) TestExecuteTask_ExceedsMaxActionsPerExecution() {
	startTime := timestamppb.New(s.timeSource.Now())
	var bufferedStarts []*schedulespb.BufferedStart
	maxStarts := scheduler.DefaultTweakables.MaxActionsPerExecution
	for i := range maxStarts * 2 {
		bufferedStarts = append(bufferedStarts,
			&schedulespb.BufferedStart{
				NominalTime:   startTime,
				ActualTime:    startTime,
				DesiredTime:   startTime,
				Manual:        false,
				RequestId:     fmt.Sprintf("req-%d", i),
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
				Attempt:       1,
			})
	}

	// Expect up to the maximum buffered start limit to result in workflow
	// executions.
	s.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(maxStarts).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	s.runExecuteTestCase(&executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   maxStarts,
		ExpectedRunningWorkflows: maxStarts,
		ExpectedActionCount:      int64(maxStarts),
	})
}

func (s *invokerExecuteTaskSuite) runExecuteTestCase(c *executeTestCase) {
	ctx := s.newMutableContext()
	invoker, err := s.scheduler.Invoker.Get(ctx)
	s.NoError(err)

	// Set up initial state
	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows
	s.scheduler.Info.RunningWorkflows = c.InitialRunningWorkflows

	// Set LastProcessedTime to current time to ensure time checks pass
	invoker.LastProcessedTime = timestamppb.New(s.timeSource.Now())

	// Set expectations. The read and update calls will also update the Scheduler
	// component, within the same transition.
	s.ExpectReadComponent(invoker)
	s.ExpectUpdateComponent(invoker)

	// Clear old tasks and run the execute task.
	s.addedTasks = make([]tasks.Task, 0)

	// Create engine context for side effect task execution
	engineCtx := s.newEngineContext()
	err = s.executor.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	s.NoError(err)
	_, err = s.node.CloseTransaction()
	s.NoError(err)

	// Validate the results
	s.Equal(c.ExpectedBufferedStarts, len(invoker.GetBufferedStarts()))
	s.Equal(c.ExpectedRunningWorkflows, len(s.scheduler.Info.RunningWorkflows))
	s.Equal(c.ExpectedTerminateWorkflows, len(invoker.TerminateWorkflows))
	s.Equal(c.ExpectedCancelWorkflows, len(invoker.CancelWorkflows))
	s.Equal(c.ExpectedActionCount, s.scheduler.Info.ActionCount)
	s.Equal(c.ExpectedOverlapSkipped, s.scheduler.Info.OverlapSkipped)
	s.Equal(c.ExpectedMissedCatchupWindow, s.scheduler.Info.MissedCatchupWindow)

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(invoker)
	}
}

type startWorkflowExecutionRequestIDMatcher struct {
	RequestID string
}

var _ gomock.Matcher = &startWorkflowExecutionRequestIDMatcher{}

func startWorkflowExecutionRequestIDMatches(requestID string) *startWorkflowExecutionRequestIDMatcher {
	return &startWorkflowExecutionRequestIDMatcher{requestID}
}

func (s *startWorkflowExecutionRequestIDMatcher) String() string {
	return fmt.Sprintf("StartWorkflowExecutionRequest{RequestId: \"%s\"}", s.RequestID)
}

func (s *startWorkflowExecutionRequestIDMatcher) Matches(x any) bool {
	req, ok := x.(*workflowservice.StartWorkflowExecutionRequest)
	return ok && req.RequestId == s.RequestID
}
