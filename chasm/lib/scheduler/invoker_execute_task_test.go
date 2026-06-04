package scheduler_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// invokerExecuteTestEnv extends testEnv with mock clients for invoker execute tests.
type invokerExecuteTestEnv struct {
	*testEnv
	handler            *scheduler.InvokerExecuteTaskHandler
	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient  *historyservicemock.MockHistoryServiceClient
}

func newInvokerExecuteTestEnv(t *testing.T) *invokerExecuteTestEnv {
	env := newTestEnv(t, withMockEngine())

	mockFrontendClient := workflowservicemock.NewMockWorkflowServiceClient(env.Ctrl)
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(env.Ctrl)

	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		HistoryClient:  mockHistoryClient,
		FrontendClient: mockFrontendClient,
	})

	return &invokerExecuteTestEnv{
		testEnv:            env,
		handler:            handler,
		mockFrontendClient: mockFrontendClient,
		mockHistoryClient:  mockHistoryClient,
	}
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

	ValidateInvoker func(t *testing.T, invoker *scheduler.Invoker, env *invokerExecuteTestEnv)
}

func runExecuteTestCase(t *testing.T, env *invokerExecuteTestEnv, c *executeTestCase) {
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	// Set up initial state. Note: InitialRunningWorkflows is now represented by
	// BufferedStarts that have RunId set but no Completed field.
	invoker.BufferedStarts = c.InitialBufferedStarts
	invoker.CancelWorkflows = c.InitialCancelWorkflows
	invoker.TerminateWorkflows = c.InitialTerminateWorkflows

	// Add initial running workflows as BufferedStarts with RunId set.
	for _, wf := range c.InitialRunningWorkflows {
		invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
			RequestId:  wf.WorkflowId + "-req",
			WorkflowId: wf.WorkflowId,
			RunId:      wf.RunId,
			Attempt:    1,
		})
	}

	// Set LastProcessedTime to current time to ensure time checks pass.
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	// Set expectations. The read and update calls will also update the Scheduler
	// component, within the same transition.
	env.ExpectReadComponent(ctx, invoker)
	env.ExpectUpdateComponent(ctx, invoker)

	// Create engine context for side effect task execution.
	engineCtx := env.EngineContext()
	err := env.handler.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())

	// Validate the results.
	// BufferedStarts now includes both pending and running starts (they're kept after starting).
	require.Len(t, invoker.GetBufferedStarts(), c.ExpectedBufferedStarts)

	// Count running workflows from BufferedStarts (has RunId but no Completed).
	runningCount := 0
	for _, start := range invoker.GetBufferedStarts() {
		if start.GetRunId() != "" && start.GetCompleted() == nil {
			runningCount++
		}
	}
	require.Equal(t, c.ExpectedRunningWorkflows, runningCount)

	require.Len(t, invoker.TerminateWorkflows, c.ExpectedTerminateWorkflows)
	require.Len(t, invoker.CancelWorkflows, c.ExpectedCancelWorkflows)
	require.Equal(t, c.ExpectedActionCount, env.Scheduler.Info.ActionCount)
	require.Equal(t, c.ExpectedOverlapSkipped, env.Scheduler.Info.OverlapSkipped)
	require.Equal(t, c.ExpectedMissedCatchupWindow, env.Scheduler.Info.MissedCatchupWindow)

	// Callbacks.
	if c.ValidateInvoker != nil {
		c.ValidateInvoker(t, invoker, env)
	}
}

// Execute success case.
func TestExecuteTask_Basic(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
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
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(2).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// After execution, both BufferedStarts are kept (with RunId set).
	// They become "running" workflows.
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   2, // kept after starting
		ExpectedRunningWorkflows: 2,
		ExpectedActionCount:      2,
	})
}

// Execute is scheduled with an empty buffer.
func TestExecuteTask_Empty(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: nil,
	})
}

// A buffered start fails with a retryable error.
func TestExecuteTask_RetryableFailure(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)

	// Set up the Invoker's buffer with a two starts. One will succeed immediately,
	// one will fail.
	startTime := timestamppb.New(env.TimeSource.Now())
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
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("fail")).
		Times(1).
		Return(nil, serviceerror.NewDeadlineExceeded("deadline exceeded"))
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("pass")).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// After execution:
	// - Failed start stays in buffer with backoff (pending)
	// - Successful start stays in buffer with RunId set (running)
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   2, // both kept: 1 failed (backoff) + 1 running
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker, env *invokerExecuteTestEnv) {
			// Find the failed start (no RunId, has backoff).
			for _, start := range invoker.BufferedStarts {
				if start.GetRunId() == "" {
					backoffTime := start.BackoffTime.AsTime()
					require.True(t, backoffTime.After(env.TimeSource.Now()))
					require.Equal(t, int64(2), start.Attempt)
					return
				}
			}
			require.Fail(t, "expected to find failed start with backoff")
		},
	})
}

// A buffered start fails when a duplicate workflow has already been started.
func TestExecuteTask_AlreadyStarted(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
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
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(nil, serviceerror.NewWorkflowExecutionAlreadyStarted("workflow already started", "", ""))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
	})
}

// A buffered start fails from having exceeded its maximum retry limit.
func TestExecuteTask_ExceedsMaxAttempts(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
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

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
	})
}

// An execute task runs with cancels/terminations queued, which fail to execute.
func TestExecuteTask_CancelTerminateFailure(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
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
	env.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))
	env.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, serviceerror.NewInternal("internal failure"))

	// Terminate and Cancel are both attempted only once. Regardless of the service
	// call's outcome, they should have been removed from the Invoker's queue.
	runExecuteTestCase(t, env, &executeTestCase{
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
func TestExecuteTask_CancelTerminateSucceed(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
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
	env.mockHistoryClient.EXPECT().RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)
	env.mockHistoryClient.EXPECT().TerminateWorkflowExecution(gomock.Any(), gomock.Any()).Times(1).
		Return(nil, nil)

	runExecuteTestCase(t, env, &executeTestCase{
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
func TestExecuteTask_ExceedsMaxActionsPerExecution(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
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
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(maxStarts).
		Return(&workflowservice.StartWorkflowExecutionResponse{
			RunId: "run-id",
		}, nil)

	// All BufferedStarts are kept: maxStarts get RunId set (running), the rest stay pending.
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   maxStarts * 2, // all kept: started + pending
		ExpectedRunningWorkflows: maxStarts,     // only started ones
		ExpectedActionCount:      int64(maxStarts),
	})
}

// Two concurrent ExecuteTasks can both clone the invoker before either
// commits, both fire StartWorkflow for the same RequestId, and both observe
// success (server dedupes by RequestId). The losing commit must not
// double-count, stomp the winner's RunId/StartTime/HasCallback, or rewind
// Attempt/BackoffTime on the already-running entry.
func TestExecuteTask_RecordResultIdempotentOnRace(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	startTime := timestamppb.New(env.TimeSource.Now())
	winning := "winning-run"
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: startTime,
		ActualTime:  startTime,
		DesiredTime: startTime,
		RequestId:   "req",
		WorkflowId:  "wf",
		Attempt:     1,
		RunId:       winning,
		StartTime:   startTime,
		HasCallback: true,
	}}
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	loserStartTime := timestamppb.New(env.TimeSource.Now().Add(time.Second))
	loser := []*schedulespb.BufferedStart{{
		RequestId: "req",
		RunId:     "loser-run",
		StartTime: loserStartTime,
	}}

	newlyStarted, droppedDuplicates := invoker.RecordExecuteResult(ctx, loser, nil)
	require.Equal(t, 0, newlyStarted, "duplicate RunId-set start must not be counted")
	require.Equal(t, 1, droppedDuplicates, "the dropped completion must be reported for observability")
	live := invoker.BufferedStarts[0]
	require.Equal(t, winning, live.RunId, "live RunId must not be stomped")
	require.Equal(t, startTime.AsTime(), live.StartTime.AsTime(), "live StartTime must not be stomped")
	require.True(t, live.HasCallback, "live HasCallback must not be cleared")

	// First-mover case: a CompletedStart for a fresh RequestId increments
	// newlyStarted and writes through to the live entry.
	invoker.BufferedStarts = append(invoker.BufferedStarts, &schedulespb.BufferedStart{
		NominalTime: startTime,
		ActualTime:  startTime,
		DesiredTime: startTime,
		RequestId:   "req2",
		WorkflowId:  "wf2",
		Attempt:     1,
	})
	first := []*schedulespb.BufferedStart{{
		RequestId: "req2",
		RunId:     "first-run",
		StartTime: startTime,
	}}
	newlyStarted, droppedDuplicates = invoker.RecordExecuteResult(ctx, first, nil)
	require.Equal(t, 1, newlyStarted, "first-time RunId assignment must be counted")
	require.Equal(t, 0, droppedDuplicates, "no duplicate was dropped")
	freshlyStarted := invoker.BufferedStarts[1]
	require.Equal(t, "first-run", freshlyStarted.RunId)
	require.Equal(t, startTime.AsTime(), freshlyStarted.StartTime.AsTime())
	require.True(t, freshlyStarted.HasCallback, "first-time RunId assignment must set HasCallback")
}

// A RetryableStart for a request whose live BufferedStart already has RunId
// set must not bump Attempt or set BackoffTime - the same RunId guard that
// protects the completed branch must also protect the retryable branch,
// otherwise stale retry metadata persists on an already-running entry.
func TestExecuteTask_RecordResultIdempotentOnRetryableRace(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	startTime := timestamppb.New(env.TimeSource.Now())
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime: startTime,
		ActualTime:  startTime,
		DesiredTime: startTime,
		RequestId:   "req",
		WorkflowId:  "wf",
		Attempt:     1,
		RunId:       "winning-run",
		StartTime:   startTime,
		HasCallback: true,
	}}
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

	// A losing concurrent Execute saw the start as eligible, its RPC failed
	// retryably, and applyBackoff produced a RetryableStart entry.
	loserBackoff := timestamppb.New(env.TimeSource.Now().Add(time.Minute))
	retryable := []*schedulespb.BufferedStart{{
		RequestId:   "req",
		BackoffTime: loserBackoff,
	}}

	newlyStarted, droppedDuplicates := invoker.RecordExecuteResult(ctx, nil, retryable)
	require.Equal(t, 0, newlyStarted)
	require.Equal(t, 0, droppedDuplicates, "retryable drops aren't counted as duplicates - only completed-side drops are")
	live := invoker.BufferedStarts[0]
	require.Equal(t, int64(1), live.Attempt, "Attempt must not be incremented on a started entry")
	require.Nil(t, live.BackoffTime, "BackoffTime must not be set on a started entry")
}

// A start whose BackoffTime exactly equals LastProcessedTime must be eligible.
// Regression for the strict-Before check, which excluded equal-time entries
// and stranded retries that landed precisely at the HWM boundary.
func TestExecuteTask_Validate_BackoffEqualToLPTIsEligible(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)

	now := env.TimeSource.Now()
	invoker.LastProcessedTime = timestamppb.New(now)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId:   "boundary",
		Attempt:     2,
		BackoffTime: timestamppb.New(now),
	}}

	valid, err := env.handler.Validate(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.True(t, valid, "BackoffTime == LastProcessedTime must be eligible (<=, not strict <)")
}

// BackoffTime must be derived from the framework clock (chasm.Context.Now),
// not wall-clock time.Now. Test by advancing TimeSource so it diverges from
// the wall clock, then asserting BackoffTime falls within the framework
// clock's frame.
func TestExecuteTask_BackoffUsesFrameworkClock(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	frameworkNow := env.TimeSource.Now().Add(24 * time.Hour)
	env.TimeSource.Update(frameworkNow)

	startTime := timestamppb.New(env.TimeSource.Now())
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Times(1).
		Return(nil, serviceerror.NewDeadlineExceeded("transient"))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			RequestId:     "retry",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       1,
		}},
		ExpectedBufferedStarts: 1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker, env *invokerExecuteTestEnv) {
			backoff := invoker.BufferedStarts[0].BackoffTime.AsTime()
			require.True(t, backoff.After(frameworkNow),
				"BackoffTime %v must be after framework clock %v", backoff, frameworkNow)
			// time.Now() + any plausible delay is far before frameworkNow (TimeSource
			// was advanced by 24h). If BackoffTime were derived from wall clock,
			// this assertion would fail.
			require.True(t, backoff.After(time.Now().Add(time.Hour)),
				"BackoffTime %v looks derived from wall clock, not framework clock", backoff)
		},
	})
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
