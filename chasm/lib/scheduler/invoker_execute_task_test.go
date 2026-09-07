package scheduler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/testing/mockapi/workflowservicemock/v1"
	"go.temporal.io/server/common/testing/testlogger"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// invokerExecuteTestEnv extends testEnv with mock clients for invoker execute tests.
type invokerExecuteTestEnv struct {
	*testEnv
	handler            *scheduler.InvokerExecuteTaskHandler
	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
	mockHistoryClient  *historyservicemock.MockHistoryServiceClient
}

func newInvokerExecuteTestEnv(t *testing.T, configure ...func(*scheduler.Config)) *invokerExecuteTestEnv {
	env := newTestEnv(t, withMockEngine())

	mockFrontendClient := workflowservicemock.NewMockWorkflowServiceClient(env.Ctrl)
	mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(env.Ctrl)

	config := defaultConfig()
	for _, fn := range configure {
		fn(config)
	}
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         config,
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

	executeTaskOnce(t, env, ctx, invoker)

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

// executeTaskOnce runs a single InvokerExecuteTask pass against the live
// Invoker and commits the resulting transition. Shared by the single-pass
// table driver (runExecuteTestCase) and by tests that need to drive several
// Execute passes in sequence.
func executeTaskOnce(t *testing.T, env *invokerExecuteTestEnv, ctx chasm.MutableContext, invoker *scheduler.Invoker) {
	t.Helper()

	// Set expectations. The read and update calls will also update the Scheduler
	// component, within the same transition.
	env.ExpectReadComponent(ctx, invoker)
	env.ExpectUpdateComponent(ctx, invoker)

	// Create engine context for side effect task execution.
	engineCtx := env.EngineContext()
	err := env.handler.Execute(engineCtx, chasm.ComponentRef{}, chasm.TaskAttributes{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.NoError(t, env.CloseTransaction())
}

// Execute success case.
func TestExecuteTask_Basic(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	env.Scheduler.LastCompletionResult = chasm.NewDataField(env.MutableContext(), &schedulerpb.LastCompletionResult{
		Success: &commonpb.Payload{Data: []byte("previous-result")},
		Failure: &failurepb.Failure{Message: "previous-failure"},
	})
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
		DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Len(t, req.GetCompletionCallbacks(), 1)
			require.Empty(t, req.GetLastCompletionResult().GetPayloads())
			require.Nil(t, req.GetContinuedFailure())
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil
		}).
		Times(2)

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      2,
		ValidateInvoker: func(t *testing.T, _ *scheduler.Invoker, env *invokerExecuteTestEnv) {
			listInfo := env.Scheduler.ListInfo(env.ReadContext())
			require.Len(t, listInfo.GetRecentActions(), 2)
			for _, action := range listInfo.GetRecentActions() {
				require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, action.GetStartWorkflowStatus())
			}
		},
	})
}

func TestExecuteTask_AllowAllRemovedAcrossEngineTransaction(t *testing.T) {
	logger := testlogger.NewTestLogger(t, testlogger.FailOnExpectedErrorOnly)
	engine, engineCtx := newTestEngineContext(t, logger)
	result, err := chasm.StartExecution(
		engineCtx,
		chasm.ExecutionKey{NamespaceID: namespaceID, BusinessID: scheduleID},
		scheduler.CreateScheduler,
		&schedulerpb.CreateScheduleRequest{
			NamespaceId: namespaceID,
			FrontendRequest: &workflowservice.CreateScheduleRequest{
				Namespace:  namespace,
				ScheduleId: scheduleID,
				Schedule:   defaultSchedule(),
			},
		},
	)
	require.NoError(t, err)
	rootRef := chasm.NewComponentRef[*scheduler.Scheduler](result.ExecutionKey)

	startTime := timestamppb.Now()
	var invoker *scheduler.Invoker
	_, _, err = chasm.UpdateComponent(
		engineCtx,
		rootRef,
		func(sched *scheduler.Scheduler, ctx chasm.MutableContext, _ struct{}) (struct{}, error) {
			invoker = sched.Invoker.Get(ctx)
			invoker.LastProcessedTime = startTime
			invoker.BufferedStarts = []*schedulespb.BufferedStart{{
				NominalTime: startTime, ActualTime: startTime, DesiredTime: startTime,
				RequestId: "allow-all-request", WorkflowId: "allow-all-workflow",
				OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, Attempt: 1,
			}}
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	frontendClient := workflowservicemock.NewMockWorkflowServiceClient(ctrl)
	frontendClient.EXPECT().StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Len(t, req.GetCompletionCallbacks(), 1)
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "allow-all-run"}, nil
		})
	handler := scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         defaultConfig(),
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     logger,
		FrontendClient: frontendClient,
	})

	dropped, err := chasmtest.ExecuteSideEffectTask(
		context.Background(),
		engine,
		invoker,
		handler,
		chasm.TaskAttributes{},
		&schedulerpb.InvokerExecuteTask{},
	)
	require.NoError(t, err)
	require.False(t, dropped)

	_, err = chasm.ReadComponent(
		engineCtx,
		rootRef,
		func(sched *scheduler.Scheduler, ctx chasm.Context, _ struct{}) (struct{}, error) {
			persistedInvoker := sched.Invoker.Get(ctx)
			require.Empty(t, persistedInvoker.GetBufferedStarts())
			require.Len(t, sched.Info.GetRecentActions(), 1)
			require.Equal(t, "allow-all-run", sched.Info.GetRecentActions()[0].GetStartWorkflowResult().GetRunId())
			describe, describeErr := sched.Describe(ctx, &schedulerpb.DescribeScheduleRequest{}, newLegacySpecBuilder(0, 0))
			require.NoError(t, describeErr)
			require.Empty(t, describe.GetFrontendResponse().GetInfo().GetRunningWorkflows())
			require.Len(t, describe.GetFrontendResponse().GetInfo().GetRecentActions(), 1)
			require.Zero(t, describe.GetFrontendResponse().GetInfo().GetBufferSize())
			return struct{}{}, nil
		},
		struct{}{},
	)
	require.NoError(t, err)
}

func TestRecordExecuteResult_AllowAllRecentActionsBounded(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	base := env.TimeSource.Now().Add(-time.Hour)
	var completed []*schedulespb.BufferedStart
	for idx := range scheduler.RecentActionCount + 2 {
		startTime := timestamppb.New(base.Add(time.Duration(idx) * time.Minute))
		start := &schedulespb.BufferedStart{
			NominalTime: startTime, ActualTime: startTime, DesiredTime: startTime,
			RequestId: fmt.Sprintf("req-%d", idx), WorkflowId: fmt.Sprintf("wf-%d", idx),
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL, Attempt: 1,
		}
		invoker.BufferedStarts = append(invoker.BufferedStarts, start)
		result := proto.Clone(start).(*schedulespb.BufferedStart)
		result.RunId = fmt.Sprintf("run-%d", idx)
		result.StartTime = startTime
		completed = append(completed, result)
	}

	newlyStarted, droppedDuplicates, startOnlyActions := invoker.RecordExecuteResult(ctx, completed, nil)
	env.Scheduler.RecordStartOnlyActions(ctx, startOnlyActions)
	require.Equal(t, scheduler.RecentActionCount+2, newlyStarted)
	require.Zero(t, droppedDuplicates)
	require.Empty(t, invoker.GetBufferedStarts())
	require.Len(t, env.Scheduler.Info.GetRecentActions(), scheduler.RecentActionCount)
	require.Equal(t, "run-2", env.Scheduler.Info.GetRecentActions()[0].GetStartWorkflowResult().GetRunId())
	require.Equal(t, "run-11", env.Scheduler.Info.GetRecentActions()[scheduler.RecentActionCount-1].GetStartWorkflowResult().GetRunId())
	recentRunIDs := make(map[string]struct{}, scheduler.RecentActionCount)
	for _, action := range env.Scheduler.Info.GetRecentActions() {
		recentRunIDs[action.GetStartWorkflowResult().GetRunId()] = struct{}{}
	}
	require.NotContains(t, recentRunIDs, "run-0")
	bufferedRequestIDs := make(map[string]struct{}, len(invoker.GetBufferedStarts()))
	for _, start := range invoker.GetBufferedStarts() {
		bufferedRequestIDs[start.GetRequestId()] = struct{}{}
	}
	require.NotContains(t, bufferedRequestIDs, "req-0")
	require.Len(t, env.Scheduler.ListInfo(ctx).GetRecentActions(), 5)
}

func TestExecuteTask_ForwardsVersioningOverride(t *testing.T) {
	tests := map[string]struct {
		override *workflowpb.VersioningOverride
		enabled  bool
	}{
		"pinned": {
			enabled: true,
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_Pinned{
					Pinned: &workflowpb.VersioningOverride_PinnedOverride{
						Behavior: workflowpb.VersioningOverride_PINNED_OVERRIDE_BEHAVIOR_PINNED,
						Version: &deploymentpb.WorkerDeploymentVersion{
							DeploymentName: "deployment",
							BuildId:        "build-id",
						},
					},
				},
			},
		},
		"one-time": {
			enabled: true,
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_OneTime{
					OneTime: &workflowpb.VersioningOverride_OneTimeOverride{
						TargetDeploymentVersion: &deploymentpb.WorkerDeploymentVersion{
							DeploymentName: "deployment",
							BuildId:        "build-id",
						},
					},
				},
			},
		},
		"disabled": {
			override: &workflowpb.VersioningOverride{
				Override: &workflowpb.VersioningOverride_AutoUpgrade{AutoUpgrade: true},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			env := newInvokerExecuteTestEnv(t, func(config *scheduler.Config) {
				tweakables := scheduler.DefaultTweakables
				tweakables.EnableVersioningOverride = test.enabled
				config.Tweakables = func(string) scheduler.Tweakables { return tweakables }
			})
			env.Scheduler.GetSchedule().GetAction().GetStartWorkflow().VersioningOverride = test.override
			startTime := timestamppb.New(env.TimeSource.Now())

			env.mockFrontendClient.EXPECT().
				StartWorkflowExecution(gomock.Any(), gomock.Any()).
				DoAndReturn(func(_ context.Context, request *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
					var expected *workflowpb.VersioningOverride
					if test.enabled {
						expected = test.override
					}
					require.True(t, proto.Equal(expected, request.GetVersioningOverride()))
					return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil
				})

			runExecuteTestCase(t, env, &executeTestCase{
				InitialBufferedStarts: []*schedulespb.BufferedStart{{
					NominalTime:   startTime,
					ActualTime:    startTime,
					DesiredTime:   startTime,
					RequestId:     "request-id",
					OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
					Attempt:       1,
				}},
				ExpectedBufferedStarts:   0,
				ExpectedRunningWorkflows: 0,
				ExpectedActionCount:      1,
			})
		})
	}
}

func TestExecuteTask_DistinctRequestsCanReuseCompletedWorkflowID(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())
	bufferedStarts := []*schedulespb.BufferedStart{
		{
			NominalTime: startTime,
			ActualTime:  startTime,
			DesiredTime: startTime,
			RequestId:   "automatic-request",
			WorkflowId:  "workflow-id",
			Attempt:     1,
			RunId:       "completed-run",
			StartTime:   startTime,
			Completed:   &schedulespb.CompletedResult{},
		},
		{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        true,
			RequestId:     "backfill-request",
			WorkflowId:    "workflow-id",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
			Attempt:       1,
		},
	}

	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("backfill-request")).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "backfill-run"}, nil)

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   2,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker, _ *invokerExecuteTestEnv) {
			require.Equal(t, "completed-run", invoker.BufferedStarts[0].GetRunId())
			require.Equal(t, "backfill-run", invoker.BufferedStarts[1].GetRunId())
		},
	})
}

func TestExecuteTask_UsesBufferedOverlapPolicy(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	env.Scheduler.Schedule.Policies.OverlapPolicy = enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL
	startTime := timestamppb.New(env.TimeSource.Now())

	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, req *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			require.Len(t, req.GetCompletionCallbacks(), 1)
			return &workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil
		})

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: startTime, ActualTime: startTime, DesiredTime: startTime,
			RequestId: "request-id", WorkflowId: "workflow-id", Attempt: 1,
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL,
		}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
		ValidateInvoker: func(t *testing.T, invoker *scheduler.Invoker, _ *invokerExecuteTestEnv) {
			require.Equal(t, enumspb.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL, invoker.BufferedStarts[0].GetOverlapPolicy())
		},
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
	// - Successful ALLOW_ALL start moves to recent-action history
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 0,
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
// InvokerMaxStartAttempts is an inclusive bound on the 1-based attempt number,
// so the first attempt that is refused locally (zero RPCs, start dropped) is
// the one past the limit.
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
			Attempt:       scheduler.InvokerMaxStartAttempts + 1,
		},
	}

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      0,
	})
}

// Boundary control: a start sitting at exactly InvokerMaxStartAttempts is still
// within budget, so its RPC is issued and a success there is recorded normally.
func TestExecuteTask_SucceedsOnFinalAttempt(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	startTime := timestamppb.New(env.TimeSource.Now())

	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("final")).
		Times(1).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run-id"}, nil)

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			Manual:        false,
			RequestId:     "final",
			WorkflowId:    "wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       scheduler.InvokerMaxStartAttempts,
		}},
		ExpectedBufferedStarts:   0,
		ExpectedRunningWorkflows: 0,
		ExpectedActionCount:      1,
	})
}

// InvokerMaxStartAttempts is documented as the maximum number of start
// attempts, so a continuously retryable start must produce exactly that many
// StartWorkflowExecution RPCs before it is dropped. Attempts are 1-based:
// recordProcessBufferResult readies a start at Attempt 1, so the bound has to
// be inclusive for the constant to mean what it says.
//
// This drives the real task order for a single automatic start - ProcessBuffer
// promotes it, then ProcessBuffer (advancing the high-water mark past each
// persisted backoff) and Execute alternate - and counts the RPCs.
func TestExecuteTask_RetriesUpToMaxStartAttempts(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	processBufferHandler := newProcessBufferHandler(env.testEnv)

	var rpcCount int
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), startWorkflowExecutionRequestIDMatches("retry")).
		AnyTimes().
		DoAndReturn(func(_ context.Context, _ *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			rpcCount++
			return nil, serviceerror.NewDeadlineExceeded("transient")
		})

	// Seed a single automatic, unprocessed buffered start (Attempt == 0), as the
	// Generator would.
	startTime := timestamppb.New(env.TimeSource.Now())
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime:   startTime,
		ActualTime:    startTime,
		DesiredTime:   startTime,
		Manual:        false,
		RequestId:     "retry",
		WorkflowId:    "wf",
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
	}}
	invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())
	require.NoError(t, env.CloseTransaction())

	// Attempt numbers observed at the head of each Execute pass.
	var attempted []int64
	maxIterations := 2*scheduler.InvokerMaxStartAttempts + 5
	iterations := 0
	for ; iterations < maxIterations; iterations++ {
		starts := env.Scheduler.Invoker.Get(env.ReadContext()).GetBufferedStarts()
		if len(starts) == 0 {
			// The start was dropped: the retry budget is spent.
			break
		}

		// Advance the framework clock to the persisted backoff deadline, which is
		// where addTasks scheduled the next ProcessBuffer timer. This has to happen
		// before the transaction's context is created, since chasm.Context.Now is
		// captured at construction - as it is when the real timer task fires at its
		// deadline. BackoffTime is framework-clock derived (see
		// TestExecuteTask_BackoffUsesFrameworkClock), so the EventTimeSource is the
		// only clock involved.
		if backoffTime := starts[0].GetBackoffTime(); backoffTime != nil &&
			backoffTime.AsTime().After(env.TimeSource.Now()) {
			env.TimeSource.Update(backoffTime.AsTime())
		}

		ctx := env.MutableContext()
		invoker := env.Scheduler.Invoker.Get(ctx)
		require.NoError(t, processBufferHandler.Execute(ctx, invoker, chasm.TaskAttributes{}, &schedulerpb.InvokerProcessBufferTask{}))
		require.NoError(t, env.CloseTransaction())

		ctx = env.MutableContext()
		invoker = env.Scheduler.Invoker.Get(ctx)
		if starts := invoker.GetBufferedStarts(); len(starts) > 0 {
			attempted = append(attempted, starts[0].GetAttempt())
		}
		executeTaskOnce(t, env, ctx, invoker)
	}
	require.Less(t, iterations, maxIterations, "retry loop never terminated")

	require.Equal(t, scheduler.InvokerMaxStartAttempts, rpcCount,
		"a continuously retryable start must issue exactly InvokerMaxStartAttempts StartWorkflowExecution RPCs")

	// The executed attempt numbers must be a contiguous 1..InvokerMaxStartAttempts
	// run, followed by the local refusal pass that drops the start.
	var expectedAttempts []int64
	for i := int64(1); i <= scheduler.InvokerMaxStartAttempts+1; i++ {
		expectedAttempts = append(expectedAttempts, i)
	}
	require.Equal(t, expectedAttempts, attempted)

	ctx = env.MutableContext()
	invoker = env.Scheduler.Invoker.Get(ctx)
	require.Empty(t, invoker.GetBufferedStarts(), "the exhausted start must be dropped from the buffer")
	require.Equal(t, int64(0), env.Scheduler.Info.ActionCount, "no action was ever started")
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

	// Started ALLOW_ALL actions move to recent history; only the unexecuted half remains buffered.
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts:    bufferedStarts,
		ExpectedBufferedStarts:   maxStarts,
		ExpectedRunningWorkflows: 0,
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
	lastEventTime := env.Scheduler.LastEventTime.AsTime()

	newlyStarted, droppedDuplicates, startOnlyActions := invoker.RecordExecuteResult(ctx, loser, nil)
	require.Equal(t, 0, newlyStarted, "duplicate RunId-set start must not be counted")
	require.Equal(t, 1, droppedDuplicates, "the dropped completion must be reported for observability")
	require.Empty(t, startOnlyActions)
	live := invoker.BufferedStarts[0]
	require.Equal(t, winning, live.RunId, "live RunId must not be stomped")
	require.Equal(t, startTime.AsTime(), live.StartTime.AsTime(), "live StartTime must not be stomped")
	require.True(t, live.HasCallback, "live HasCallback must not be cleared")
	require.Equal(t, lastEventTime, env.Scheduler.LastEventTime.AsTime(),
		"duplicate result must not advance the last-event high water mark")

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
	newlyStarted, droppedDuplicates, startOnlyActions = invoker.RecordExecuteResult(ctx, first, nil)
	require.Equal(t, 1, newlyStarted, "first-time RunId assignment must be counted")
	require.Equal(t, 0, droppedDuplicates, "no duplicate was dropped")
	require.Empty(t, startOnlyActions)
	freshlyStarted := invoker.BufferedStarts[1]
	require.Equal(t, "first-run", freshlyStarted.RunId)
	require.Equal(t, startTime.AsTime(), freshlyStarted.StartTime.AsTime())
	require.True(t, freshlyStarted.HasCallback, "first-time RunId assignment must set HasCallback")
}

func TestRecordExecuteResult_AdvancesLastEventTimeBeforeRetention(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	base := env.TimeSource.Now().Add(time.Hour)

	var completedStarts []*schedulespb.BufferedStart
	for idx := range scheduler.RecentActionCount + 1 {
		startTime := base.Add(time.Duration(idx) * time.Second)
		bufferedStart := &schedulespb.BufferedStart{
			NominalTime: timestamppb.New(startTime),
			ActualTime:  timestamppb.New(startTime),
			DesiredTime: timestamppb.New(startTime),
			RequestId:   fmt.Sprintf("req-%d", idx),
			WorkflowId:  fmt.Sprintf("wf-%d", idx),
			Attempt:     1,
		}
		invoker.BufferedStarts = append(invoker.BufferedStarts, bufferedStart)
		completedStarts = append(completedStarts, &schedulespb.BufferedStart{
			RequestId: fmt.Sprintf("req-%d", idx),
			RunId:     fmt.Sprintf("run-%d", idx),
			StartTime: timestamppb.New(startTime),
		})
	}

	newlyStarted, droppedDuplicates, startOnlyActions := invoker.RecordExecuteResult(ctx, completedStarts, nil)
	require.Equal(t, scheduler.RecentActionCount+1, newlyStarted)
	require.Zero(t, droppedDuplicates)
	require.Empty(t, startOnlyActions)

	latestStartTime := base.Add(scheduler.RecentActionCount * time.Second)
	require.Equal(t, latestStartTime.UTC(), env.Scheduler.LastEventTime.AsTime())

	for idx, start := range invoker.BufferedStarts {
		start.Completed = &schedulespb.CompletedResult{
			Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			// The latest-started workflow closes first and is evicted by retention.
			CloseTime: timestamppb.New(base.Add(time.Duration(scheduler.RecentActionCount-idx) * time.Second)),
		}
	}
	invoker.ApplyCompletedRetention()

	require.Len(t, invoker.BufferedStarts, scheduler.RecentActionCount)
	require.NotEqual(t, latestStartTime.UTC(), invoker.BufferedStarts[len(invoker.BufferedStarts)-1].GetStartTime().AsTime())
	require.Equal(t, latestStartTime.UTC(), env.Scheduler.LastEventTime.AsTime(),
		"retention must not erase a start time captured before the Generator runs")
}

func TestRecordExecuteResult_AdvancesLastEventTimeForStartOnlyAction(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	startTime := env.TimeSource.Now().Add(time.Hour)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		NominalTime:   timestamppb.New(startTime),
		ActualTime:    timestamppb.New(startTime),
		DesiredTime:   timestamppb.New(startTime),
		RequestId:     "req",
		WorkflowId:    "wf",
		OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
		Attempt:       1,
	}}

	newlyStarted, droppedDuplicates, startOnlyActions := invoker.RecordExecuteResult(ctx, []*schedulespb.BufferedStart{{
		RequestId: "req",
		RunId:     "run",
		StartTime: timestamppb.New(startTime),
	}}, nil)

	require.Equal(t, 1, newlyStarted)
	require.Zero(t, droppedDuplicates)
	require.Len(t, startOnlyActions, 1)
	require.Empty(t, invoker.BufferedStarts)
	require.Equal(t, startTime.UTC(), env.Scheduler.LastEventTime.AsTime(),
		"start-only action must advance the mark before leaving BufferedStarts")
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

	newlyStarted, droppedDuplicates, startOnlyActions := invoker.RecordExecuteResult(ctx, nil, retryable)
	require.Equal(t, 0, newlyStarted)
	require.Equal(t, 0, droppedDuplicates, "retryable drops aren't counted as duplicates - only completed-side drops are")
	require.Empty(t, startOnlyActions)
	live := invoker.BufferedStarts[0]
	require.Equal(t, int64(1), live.Attempt, "Attempt must not be incremented on a started entry")
	require.Nil(t, live.BackoffTime, "BackoffTime must not be set on a started entry")
}

func TestExecuteTask_EventLog(t *testing.T) {
	newStart := func(requestID, runID string) *schedulespb.BufferedStart {
		return &schedulespb.BufferedStart{RequestId: requestID, RunId: runID}
	}

	cases := []struct {
		name       string
		buffered   []*schedulespb.BufferedStart
		cancels    []*commonpb.WorkflowExecution
		terminates []*commonpb.WorkflowExecution
		completed  []*schedulespb.BufferedStart
		retryable  []*schedulespb.BufferedStart
		wantLog    string
	}{
		{
			name:      "kicked off counts newly started",
			buffered:  []*schedulespb.BufferedStart{newStart("s1", "")},
			completed: []*schedulespb.BufferedStart{newStart("s1", "run-1")},
			wantLog:   "recordExecuteResult kicked off 1 starts, removed 0 starts, retried 0 starts",
		},
		{
			name:      "retried counts retryable starts",
			buffered:  []*schedulespb.BufferedStart{newStart("s1", "")},
			retryable: []*schedulespb.BufferedStart{newStart("s1", "")},
			wantLog:   "recordExecuteResult kicked off 0 starts, removed 0 starts, retried 1 starts",
		},
		{
			name:     "surviving buffered starts are not counted as removed",
			buffered: []*schedulespb.BufferedStart{newStart("a", ""), newStart("b", ""), newStart("c", "")},
			wantLog:  "recordExecuteResult kicked off 0 starts, removed 0 starts, retried 0 starts",
		},
		{
			name:    "surviving cancels are not counted as removed",
			cancels: []*commonpb.WorkflowExecution{{RunId: "c1"}, {RunId: "c2"}},
			wantLog: "recordExecuteResult kicked off 0 starts, removed 0 starts, retried 0 starts",
		},
		{
			name:       "surviving terminates are not counted as removed",
			terminates: []*commonpb.WorkflowExecution{{RunId: "t1"}},
			wantLog:    "recordExecuteResult kicked off 0 starts, removed 0 starts, retried 0 starts",
		},
		{
			name:       "survivors alongside real work count independently",
			buffered:   []*schedulespb.BufferedStart{newStart("s1", ""), newStart("s2", "")},
			cancels:    []*commonpb.WorkflowExecution{{RunId: "c1"}},
			terminates: []*commonpb.WorkflowExecution{{RunId: "t1"}},
			completed:  []*schedulespb.BufferedStart{newStart("s1", "run-1")},
			retryable:  []*schedulespb.BufferedStart{newStart("s2", "")},
			wantLog:    "recordExecuteResult kicked off 1 starts, removed 0 starts, retried 1 starts",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := newTestEnv(t)
			ctx := env.MutableContext()
			invoker := env.Scheduler.Invoker.Get(ctx)

			invoker.BufferedStarts = tc.buffered
			invoker.CancelWorkflows = tc.cancels
			invoker.TerminateWorkflows = tc.terminates
			invoker.LastProcessedTime = timestamppb.New(env.TimeSource.Now())

			eventLog := invoker.EventLog.Get(ctx)
			eventLog.Events = nil

			invoker.RecordExecuteResult(ctx, tc.completed, tc.retryable)

			var messages []string
			for _, e := range eventLog.Events {
				messages = append(messages, e.Message)
			}
			require.Contains(t, messages, tc.wantLog)
		})
	}
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

	valid, err := env.handler.Validate(ctx, invoker, chasm.TaskInvocation{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.True(t, valid, "BackoffTime == LastProcessedTime must be eligible (<=, not strict <)")
}

func TestExecuteTask_Validate_MigrationPending(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{RequestId: "ready", Attempt: 1}}
	env.Scheduler.WorkflowMigration = &schedulerpb.WorkflowMigrationState{}

	valid, err := env.handler.Validate(ctx, invoker, chasm.TaskInvocation{}, &schedulerpb.InvokerExecuteTask{})
	require.NoError(t, err)
	require.False(t, valid)
}

// Validate must skip Execute when no work is ready.
func TestExecuteTask_Validate(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := env.TimeSource.Now()

	cases := []struct {
		name          string
		setup         func()
		expectedValid bool
	}{
		{
			name:          "empty invoker is invalid",
			setup:         func() {},
			expectedValid: false,
		},
		{
			name: "eligible start is valid",
			setup: func() {
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					RequestId: "ready",
					Attempt:   1,
				}}
			},
			expectedValid: true,
		},
		{
			// When the RunID is set, the start's already been kicked off, so we don't need
			// an execute task.
			name: "running start (RunId set) is invalid",
			setup: func() {
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					RequestId: "running",
					Attempt:   1,
					RunId:     "run-1",
				}}
			},
			expectedValid: false,
		},
		{
			// A retry is pending, but we're still in exponential backoff time.
			name: "backing-off start is invalid",
			setup: func() {
				invoker.LastProcessedTime = timestamppb.New(now)
				invoker.BufferedStarts = []*schedulespb.BufferedStart{{
					RequestId:   "backoff",
					Attempt:     2,
					BackoffTime: timestamppb.New(now.Add(time.Minute)),
				}}
			},
			expectedValid: false,
		},
		{
			name: "pending cancel is valid",
			setup: func() {
				invoker.CancelWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "wf", RunId: "r"}}
			},
			expectedValid: true,
		},
		{
			name: "pending terminate is valid",
			setup: func() {
				invoker.TerminateWorkflows = []*commonpb.WorkflowExecution{{WorkflowId: "wf", RunId: "r"}}
			},
			expectedValid: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			invoker.BufferedStarts = nil
			invoker.CancelWorkflows = nil
			invoker.TerminateWorkflows = nil
			invoker.LastProcessedTime = nil
			c.setup()
			valid, err := env.handler.Validate(ctx, invoker, chasm.TaskInvocation{}, &schedulerpb.InvokerExecuteTask{})
			require.NoError(t, err)
			require.Equal(t, c.expectedValid, valid)
		})
	}
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
