package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/taskpoller"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestWorkflowHeartbeatNotificationMetricsWithoutStartedTask(t *testing.T) {
	t.Parallel()
	for _, input := range []string{"signal", "timer"} {
		t.Run(input, func(t *testing.T) {
			t.Parallel()
			env := testcore.NewEnv(t)
			tv := env.Tv()
			start, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
				RequestId:                tv.RequestID(),
				Namespace:                env.Namespace().String(),
				WorkflowId:               tv.WorkflowID(),
				WorkflowType:             tv.WorkflowType(),
				TaskQueue:                tv.TaskQueue(),
				Identity:                 tv.WorkerIdentity(),
				WorkflowExecutionTimeout: durationpb.New(time.Minute),
				WorkflowTaskTimeout:      durationpb.New(10 * time.Second),
			})
			require.NoError(t, err)
			tv = tv.WithRunID(start.GetRunId())
			capture := env.StartNamespaceMetricCapture()
			_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
				func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
					response := &workflowservice.RespondWorkflowTaskCompletedRequest{}
					if input == "timer" {
						response.Commands = []*commandpb.Command{{
							CommandType: enumspb.COMMAND_TYPE_START_TIMER,
							Attributes: &commandpb.Command_StartTimerCommandAttributes{
								StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
									TimerId:            "test-timer",
									StartToFireTimeout: durationpb.New(time.Second),
								},
							},
						}}
					}
					return response, nil
				})
			require.NoError(t, err)

			switch input {
			case "signal":
				_, err = env.FrontendClient().SignalWorkflowExecution(t.Context(), &workflowservice.SignalWorkflowExecutionRequest{
					Namespace:         env.Namespace().String(),
					WorkflowExecution: tv.WorkflowExecution(),
					SignalName:        "test-signal",
					RequestId:         uuid.NewString(),
				})
				require.NoError(t, err)
			case "timer":
				_, err = env.TaskPoller().PollAndHandleWorkflowTask(tv,
					func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
						found := false
						for _, event := range task.GetHistory().GetEvents() {
							found = found || event.GetEventType() == enumspb.EVENT_TYPE_TIMER_FIRED
						}
						require.True(t, found)
						return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
					},
					taskpoller.WithTimeout(5*time.Second),
				)
				require.NoError(t, err)
			default:
				t.Fatalf("unexpected input type %q", input)
			}
			require.Empty(t, capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name()))
			require.Empty(t, capture.Metric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name()))
		})
	}
}

func TestWorkflowHeartbeatNotificationMetricsTimerFiredDuringStartedTask(t *testing.T) {
	t.Parallel()
	env := testcore.NewEnv(t)
	tv := env.Tv()
	start, err := env.FrontendClient().StartWorkflowExecution(t.Context(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                tv.RequestID(),
		Namespace:                env.Namespace().String(),
		WorkflowId:               tv.WorkflowID(),
		WorkflowType:             tv.WorkflowType(),
		TaskQueue:                tv.TaskQueue(),
		Identity:                 tv.WorkerIdentity(),
		WorkflowExecutionTimeout: durationpb.New(time.Minute),
		WorkflowTaskTimeout:      durationpb.New(30 * time.Second),
	})
	require.NoError(t, err)
	tv = tv.WithRunID(start.GetRunId())
	capture := env.StartNamespaceMetricCapture()

	resp, err := env.TaskPoller().PollAndHandleWorkflowTask(tv,
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{
				Commands: []*commandpb.Command{{
					CommandType: enumspb.COMMAND_TYPE_START_TIMER,
					Attributes: &commandpb.Command_StartTimerCommandAttributes{
						StartTimerCommandAttributes: &commandpb.StartTimerCommandAttributes{
							TimerId:            "test-timer",
							StartToFireTimeout: durationpb.New(time.Second),
						},
					},
				}},
				ForceCreateNewWorkflowTask: true,
				ReturnNewWorkflowTask:      true,
			}, nil
		})
	require.NoError(t, err)
	require.NotNil(t, resp.GetWorkflowTask())

	await.Require(t.Context(), t, func(t *await.T) {
		buffered := capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
		require.Len(t, buffered, 1)
		require.Equal(t, "TimerFired", buffered[0].Tags["event_type"])
		first := capture.Metric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name())
		require.Len(t, first, 1)
		require.Equal(t, "TimerFired", first[0].Tags["event_type"])
	}, 10*time.Second, 100*time.Millisecond)

	_, err = env.TaskPoller().HandleWorkflowTask(tv, resp.GetWorkflowTask(),
		func(*workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
			return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
		})
	require.NoError(t, err)
}

func (s *NexusWorkflowTestSuite) TestBufferedInputMetricsWithLocalActivity(chasmEnabled bool) {
	if chasmEnabled {
		s.T().Skip("metric coverage uses the HSM Nexus operation path")
	}

	env := s.newTestEnv(false)
	ctx := s.Context()
	childID := testcore.RandomizeStr(s.T().Name() + "-child")
	activityStarted := make(chan struct{})
	releaseActivity := make(chan struct{})
	finishActivity := sync.OnceFunc(func() { close(releaseActivity) })
	defer finishActivity()
	localActivityStarted := make(chan struct{})
	releaseLocalActivity := make(chan struct{})
	finishLocalActivity := sync.OnceFunc(func() { close(releaseLocalActivity) })
	defer finishLocalActivity()
	callback := make(chan nexus.StartOperationOptions, 1)

	endpoint := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
		OnStartOperation: func(_ context.Context, _, _ string, _ *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callback <- options
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test"}, nil
		},
	})

	activity := func(ctx context.Context) error {
		close(activityStarted)
		select {
		case <-releaseActivity:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	var localActivityOnce sync.Once
	localActivity := func(ctx context.Context) error {
		localActivityOnce.Do(func() { close(localActivityStarted) })
		select {
		case <-releaseLocalActivity:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	child := func(ctx workflow.Context) error {
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return nil
	}
	parent := func(ctx workflow.Context) error {
		activityCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		activityFuture := workflow.ExecuteActivity(activityCtx, activity)
		childFuture := workflow.ExecuteChildWorkflow(workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{WorkflowID: childID}), child)
		nexusFuture := workflow.NewNexusClient(endpoint, "service").ExecuteOperation(ctx, "operation", "input", workflow.NexusOperationOptions{})
		workflow.GetSignalChannel(ctx, "begin-local-activity").Receive(ctx, nil)
		localCtx := workflow.WithLocalActivityOptions(ctx, workflow.LocalActivityOptions{
			StartToCloseTimeout: time.Minute,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		if err := workflow.ExecuteLocalActivity(localCtx, localActivity).Get(ctx, nil); err != nil {
			return err
		}
		for _, future := range []workflow.Future{activityFuture, childFuture, nexusFuture} {
			if err := future.Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}

	w := env.SdkWorker()
	w.RegisterWorkflow(parent)
	w.RegisterWorkflow(child)
	w.RegisterActivity(activity)
	w.RegisterActivity(localActivity)
	run, err := env.SdkClient().ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		TaskQueue:           env.WorkerTaskQueue(),
		WorkflowTaskTimeout: time.Minute,
	}, parent)
	s.NoError(err)
	await.Rcv(s.T(), activityStarted)
	op := await.Rcv(s.T(), callback)
	s.Await(func(s *NexusWorkflowTestSuite) {
		history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: run.GetID()})
		s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_CHILD_WORKFLOW_EXECUTION_STARTED)
		s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED)
	}, 10*time.Second, 100*time.Millisecond)
	s.Await(func(s *NexusWorkflowTestSuite) {
		history := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{WorkflowId: childID})
		s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED)
	}, 10*time.Second, 100*time.Millisecond)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "begin-local-activity", nil))
	await.Rcv(s.T(), localActivityStarted)

	capture := env.StartNamespaceMetricCapture()
	finishActivity()
	s.Await(func(s *NexusWorkflowTestSuite) {
		recordings := capture.Metric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name())
		s.Len(recordings, 1)
		if len(recordings) == 1 {
			s.Equal("ActivityTaskCompleted", recordings[0].Tags["event_type"])
		}
	}, 10*time.Second, 100*time.Millisecond)

	s.NoError(env.SdkClient().SignalWorkflow(ctx, childID, "", "finish", nil))
	s.NoError(s.sendNexusCompletionRequest(ctx, op.CallbackURL, nexusrpc.CompleteOperationOptions{
		Header: nexus.Header{commonnexus.CallbackTokenHeader: op.CallbackHeader.Get(commonnexus.CallbackTokenHeader)},
	}))
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "test-signal", nil))
	_, err = env.FrontendClient().RequestCancelWorkflowExecution(ctx, &workflowservice.RequestCancelWorkflowExecutionRequest{
		Namespace:         env.Namespace().String(),
		WorkflowExecution: &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
	})
	s.NoError(err)

	expected := map[string]int{
		"ActivityTaskCompleted":            1,
		"ChildWorkflowExecutionCompleted":  1,
		"NexusOperationCompleted":          1,
		"WorkflowExecutionSignaled":        1,
		"WorkflowExecutionCancelRequested": 1,
	}
	s.Await(func(s *NexusWorkflowTestSuite) {
		recordings := capture.Metric(metrics.WorkflowHeartbeatNotificationBufferedInput.Name())
		actual := make(map[string]int, len(recordings))
		for _, recording := range recordings {
			actual[recording.Tags["event_type"]] += int(recording.Value.(int64))
		}
		s.Equal(expected, actual)
		s.Len(capture.Metric(metrics.WorkflowHeartbeatNotificationEstimatedFirstBufferedInput.Name()), 1)
	}, 10*time.Second, 100*time.Millisecond)

	// Release the Local Activity before stopping the worker.
	finishLocalActivity()
}
