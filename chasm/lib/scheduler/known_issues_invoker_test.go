package scheduler_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/chasm/lib/scheduler"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestKnownIssue_RetryDoesNotStartAfterCatchupDeadline(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	now := env.TimeSource.Now()
	startTime := timestamppb.New(now.Add(-2 * defaultCatchupWindow))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime:   startTime,
			ActualTime:    startTime,
			DesiredTime:   startTime,
			RequestId:     "expired-retry",
			WorkflowId:    "expired-retry-wf",
			OverlapPolicy: enumspb.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL,
			Attempt:       2,
		}},
		ExpectedBufferedStarts: 0,
	})
}

func TestKnownIssue_TransientCancelFailureRemainsPending(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	target := &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}
	env.mockHistoryClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDeadlineExceeded("ambiguous cancel result"))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialCancelWorkflows:  []*commonpb.WorkflowExecution{target},
		ExpectedCancelWorkflows: 1,
	})
}

func TestKnownIssue_TransientTerminateFailureRemainsPending(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	target := &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}
	env.mockHistoryClient.EXPECT().
		TerminateWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, serviceerror.NewDeadlineExceeded("ambiguous terminate result"))

	runExecuteTestCase(t, env, &executeTestCase{
		InitialTerminateWorkflows:  []*commonpb.WorkflowExecution{target},
		ExpectedTerminateWorkflows: 1,
	})
}

func TestKnownIssue_DuplicateRetryableResultsConsumeOneAttempt(t *testing.T) {
	env := newTestEnv(t)
	ctx := env.MutableContext()
	invoker := env.Scheduler.Invoker.Get(ctx)
	now := env.TimeSource.Now()
	invoker.LastProcessedTime = timestamppb.New(now)
	invoker.BufferedStarts = []*schedulespb.BufferedStart{{
		RequestId: "request",
		Attempt:   1,
	}}
	retryable := []*schedulespb.BufferedStart{{
		RequestId:   "request",
		BackoffTime: timestamppb.New(now.Add(time.Minute)),
	}}

	invoker.RecordExecuteResult(ctx, nil, retryable)
	invoker.RecordExecuteResult(ctx, nil, retryable)

	require.Equal(t, int64(2), invoker.BufferedStarts[0].Attempt,
		"duplicate delivery of one logical failed attempt must not consume another attempt")
}

func TestKnownIssue_ActionBudgetIsSharedAcrossCancelAndStart(t *testing.T) {
	config := defaultConfig()
	config.Tweakables = func(string) scheduler.Tweakables {
		tweakables := scheduler.DefaultTweakables
		tweakables.MaxActionsPerExecution = 1
		return tweakables
	}
	env := newInvokerExecuteTestEnv(t)
	env.handler = scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		HistoryClient:  env.mockHistoryClient,
		FrontendClient: env.mockFrontendClient,
	})
	env.mockHistoryClient.EXPECT().
		RequestCancelWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(nil, nil)

	now := timestamppb.New(env.TimeSource.Now())
	runExecuteTestCase(t, env, &executeTestCase{
		InitialCancelWorkflows: []*commonpb.WorkflowExecution{{WorkflowId: "old", RunId: "old-run"}},
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: now,
			ActualTime:  now,
			DesiredTime: now,
			RequestId:   "new",
			WorkflowId:  "new-wf",
			Attempt:     1,
		}},
		ExpectedBufferedStarts: 1,
	})
}

func TestKnownIssue_ServiceCallTimeoutAppliedToStartWorkflow(t *testing.T) {
	const serviceCallTimeout = 25 * time.Millisecond
	config := defaultConfig()
	config.ServiceCallTimeout = func() time.Duration { return serviceCallTimeout }
	env := newInvokerExecuteTestEnv(t)
	env.handler = scheduler.NewInvokerExecuteTaskHandler(scheduler.InvokerTaskHandlerOptions{
		Config:         config,
		MetricsHandler: metrics.NoopMetricsHandler,
		BaseLogger:     env.Logger,
		SpecProcessor:  env.SpecProcessor,
		HistoryClient:  env.mockHistoryClient,
		FrontendClient: env.mockFrontendClient,
	})
	var (
		hasDeadline bool
		deadline    time.Time
	)
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *workflowservice.StartWorkflowExecutionRequest, _ ...grpc.CallOption) (*workflowservice.StartWorkflowExecutionResponse, error) {
			deadline, hasDeadline = ctx.Deadline()
			return nil, serviceerror.NewDeadlineExceeded("test")
		})

	now := timestamppb.New(env.TimeSource.Now())
	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: now,
			ActualTime:  now,
			DesiredTime: now,
			RequestId:   "request",
			WorkflowId:  "workflow",
			Attempt:     1,
		}},
		ExpectedBufferedStarts: 1,
	})
	require.True(t, hasDeadline, "outbound start must carry the configured service-call deadline")
	require.WithinDuration(t, time.Now().Add(serviceCallTimeout), deadline, serviceCallTimeout)
}

func TestKnownIssue_MaxAttemptsIncludesTenthOutboundAttempt(t *testing.T) {
	env := newInvokerExecuteTestEnv(t)
	env.mockFrontendClient.EXPECT().
		StartWorkflowExecution(gomock.Any(), gomock.Any()).
		Return(&workflowservice.StartWorkflowExecutionResponse{RunId: "run"}, nil)
	now := timestamppb.New(env.TimeSource.Now())

	runExecuteTestCase(t, env, &executeTestCase{
		InitialBufferedStarts: []*schedulespb.BufferedStart{{
			NominalTime: now,
			ActualTime:  now,
			DesiredTime: now,
			RequestId:   "tenth-attempt",
			WorkflowId:  "workflow",
			Attempt:     scheduler.InvokerMaxStartAttempts,
		}},
		ExpectedBufferedStarts:   1,
		ExpectedRunningWorkflows: 1,
		ExpectedActionCount:      1,
	})
}
