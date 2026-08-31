package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NexusCancelTestSuite verifies exactly which scenarios cause the handler to receive a
// CancelOperation *today* (i.e. with the default ABANDON close policy — the
// NexusOperationAutoClosePolicy dynamic config is intentionally left unset). It is CHASM-only:
// standalone and workflow-backed Nexus operations both live on the CHASM path, so this suite
// always assumes CHASM is enabled (unlike NexusWorkflowTestSuite it has no HSM variant).
//
// It is the empirical counterpart to the "Cancel Nexus Operation" enumeration in the design doc:
//
//	Handler IS told (CancelOperation delivered):
//	  - explicit RequestCancelNexusOperation while the caller runs, op STARTED
//	  - caller workflow cancelled and the workflow honors it (issues the cancel while still running)
//	Handler is NOT told (gaps today):
//	  - caller workflow terminated / failed / times out / completes with a STARTED op
//	  - the operation's own schedule-to-close timeout fires while the caller keeps running
//	  - caller workflow continues-as-new
//
// Standalone (SANO) equivalents live on NexusStandaloneTestSuite below. The scheduled-never-started
// case and the start-in-flight race are only observable under REQUEST_CANCEL, so they belong with
// the close-policy feature tests, not here.
//
// All cases run against an external Nexus server whose OnCancelOperation signals a channel, so
// "delivered" is a receive on that channel and "not delivered" is the absence of one.
type NexusCancelTestSuite struct {
	parallelsuite.Suite[*NexusCancelTestSuite]
}

func TestNexusCancelTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusCancelTestSuite{})
}

func (s *NexusCancelTestSuite) newTestEnv(opts ...testcore.TestOption) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true, append(
		opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, 100),
	)...)
}

// --- shared helpers -------------------------------------------------------------------------------

// requireCancelDelivered fails unless the handler receives a CancelOperation within a generous window.
func requireCancelDelivered(t *testing.T, cancelCh <-chan struct{}) {
	t.Helper()
	select {
	case <-cancelCh:
	case <-time.After(20 * time.Second):
		t.Fatal("expected the handler to receive CancelOperation, but it never did")
	}
}

// requireCancelNotDelivered fails if the handler receives a CancelOperation within the given window.
func requireCancelNotDelivered(t *testing.T, cancelCh <-chan struct{}, within time.Duration) {
	t.Helper()
	select {
	case <-cancelCh:
		t.Fatal("expected the handler NOT to receive CancelOperation, but it did")
	case <-time.After(within):
	}
}

// nexusCancelStartSchedule starts the raw-polling "workflow" and schedules a single async Nexus
// operation against endpointName, optionally with a schedule-to-close timeout.
func nexusCancelStartSchedule(
	t *testing.T,
	env *NexusTestEnv,
	endpointName string,
	startOpts client.StartWorkflowOptions,
	scheduleToClose time.Duration,
) client.WorkflowRun {
	t.Helper()
	ctx := testcore.NewContext()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, startOpts, "workflow")
	require.NoError(t, err)

	await.Require(t.Context(), t, func(c *await.T) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: startOpts.TaskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(c, err)
		if len(pollResp.TaskToken) == 0 {
			require.Fail(c, "no workflow task available yet")
			return
		}
		attrs := &commandpb.ScheduleNexusOperationCommandAttributes{
			Endpoint:  endpointName,
			Service:   "service",
			Operation: "operation",
			Input:     testcore.MustToPayload(t, "input"),
		}
		if scheduleToClose > 0 {
			attrs.ScheduleToCloseTimeout = durationpb.New(scheduleToClose)
		}
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_SCHEDULE_NEXUS_OPERATION,
				Attributes: &commandpb.Command_ScheduleNexusOperationCommandAttributes{
					ScheduleNexusOperationCommandAttributes: attrs,
				},
			}},
		})
		require.NoError(c, err)
	}, 20*time.Second, 200*time.Millisecond)

	return run
}

// nexusCancelAwaitOpState waits until the caller's single pending Nexus operation reaches state.
func nexusCancelAwaitOpState(t *testing.T, env *NexusTestEnv, run client.WorkflowRun, state enumspb.PendingNexusOperationState) {
	t.Helper()
	await.Require(t.Context(), t, func(c *await.T) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.Len(c, resp.PendingNexusOperations, 1)
		require.Equal(c, state, resp.PendingNexusOperations[0].State)
	}, 20*time.Second, 200*time.Millisecond)
}

// nexusCancelAwaitNewRunNoPendingOps waits until the workflow has continued-as-new (its current run
// ID differs from run's) and asserts the new run carries no pending Nexus operations. Continue-as-new
// starts a fresh mutable state, so the caller's pending operations are dropped rather than carried
// into the new run.
func nexusCancelAwaitNewRunNoPendingOps(t *testing.T, env *NexusTestEnv, run client.WorkflowRun) {
	t.Helper()
	await.Require(t.Context(), t, func(c *await.T) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.NotEqual(c, run.GetRunID(), resp.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
			"expected a new run after continue-as-new")
		require.Empty(c, resp.PendingNexusOperations,
			"continue-as-new should not carry pending Nexus operations into the new run")
	}, 20*time.Second, 200*time.Millisecond)
}

// nexusCancelPollAndRequestCancel polls for a workflow task (which the caller must have triggered,
// e.g. via a signal or a cancel request), locates the NexusOperationScheduled event, and responds
// with a RequestCancelNexusOperation command. This is the explicit-cancel path.
func nexusCancelPollAndRequestCancel(t *testing.T, env *NexusTestEnv, taskQueue string) {
	t.Helper()
	ctx := testcore.NewContext()
	await.Require(t.Context(), t, func(c *await.T) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(c, err)
		if len(pollResp.TaskToken) == 0 {
			require.Fail(c, "no workflow task available yet")
			return
		}
		var scheduledEvent *historypb.HistoryEvent
		for _, e := range pollResp.History.GetEvents() {
			if e.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
				scheduledEvent = e
				break
			}
		}
		require.NotNil(c, scheduledEvent, "NexusOperationScheduled event not found in history")
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands: []*commandpb.Command{{
				CommandType: enumspb.COMMAND_TYPE_REQUEST_CANCEL_NEXUS_OPERATION,
				Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
					RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
						ScheduledEventId: scheduledEvent.GetEventId(),
					},
				},
			}},
		})
		require.NoError(c, err)
	}, 20*time.Second, 200*time.Millisecond)
}

// nexusCancelPollAndRespondClose polls for a workflow task and responds with the given close command.
func nexusCancelPollAndRespondClose(t *testing.T, env *NexusTestEnv, taskQueue string, command *commandpb.Command) {
	t.Helper()
	ctx := testcore.NewContext()
	await.Require(t.Context(), t, func(c *await.T) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(c, err)
		if len(pollResp.TaskToken) == 0 {
			require.Fail(c, "no workflow task available yet")
			return
		}
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
			Commands:  []*commandpb.Command{command},
		})
		require.NoError(c, err)
	}, 20*time.Second, 200*time.Millisecond)
}

// nexusCancelPollAndRespondEmpty polls for a workflow task and completes it with no commands, which
// records a WorkflowTaskCompleted event usable as a reset point.
func nexusCancelPollAndRespondEmpty(t *testing.T, env *NexusTestEnv, taskQueue string) {
	t.Helper()
	ctx := testcore.NewContext()
	await.Require(t.Context(), t, func(c *await.T) {
		pollResp, err := env.FrontendClient().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: env.Namespace().String(),
			TaskQueue: &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Identity:  "test",
		})
		require.NoError(c, err)
		if len(pollResp.TaskToken) == 0 {
			require.Fail(c, "no workflow task available yet")
			return
		}
		_, err = env.FrontendClient().RespondWorkflowTaskCompleted(ctx, &workflowservice.RespondWorkflowTaskCompletedRequest{
			Identity:  "test",
			TaskToken: pollResp.TaskToken,
		})
		require.NoError(c, err)
	}, 20*time.Second, 200*time.Millisecond)
}

// nexusCancelWFTCompletedEventID returns the first or last WorkflowTaskCompleted event ID of run's
// history. Passed to reset as WorkflowTaskFinishEventId, it rebuilds through the preceding event —
// so the first one drops an operation scheduled by that very task, and the last one adopts it.
func nexusCancelWFTCompletedEventID(t *testing.T, env *NexusTestEnv, run client.WorkflowRun, first bool) int64 {
	t.Helper()
	hist := env.GetHistory(env.Namespace().String(), &commonpb.WorkflowExecution{
		WorkflowId: run.GetID(), RunId: run.GetRunID(),
	})
	var eventID int64
	for _, e := range hist {
		if e.GetEventType() != enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED {
			continue
		}
		eventID = e.GetEventId()
		if first {
			break
		}
	}
	require.NotZero(t, eventID, "no WorkflowTaskCompleted event to reset to")
	return eventID
}

// nexusCancelReset resets run to the given WorkflowTaskCompleted event.
func nexusCancelReset(t *testing.T, env *NexusTestEnv, run client.WorkflowRun, wftCompletedEventID int64) {
	t.Helper()
	_, err := env.FrontendClient().ResetWorkflowExecution(testcore.NewContext(), &workflowservice.ResetWorkflowExecutionRequest{
		Namespace:                 env.Namespace().String(),
		WorkflowExecution:         &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()},
		Reason:                    "nexus auto-close reset test",
		RequestId:                 uuid.NewString(),
		WorkflowTaskFinishEventId: wftCompletedEventID,
	})
	require.NoError(t, err)
}

// nexusCancelEnv builds a CHASM env with the external cancel-observing handler and returns the env,
// a random task queue, and the endpoint name.
func (s *NexusCancelTestSuite) nexusCancelEnv(cancelCh chan struct{}, opts ...testcore.TestOption) (env *NexusTestEnv, taskQueue, endpointName string) {
	env = s.newTestEnv(opts...)
	taskQueue = testcore.RandomizeStr(s.T().Name())
	endpointName = env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))
	return env, taskQueue, endpointName
}

// nexusCancelBlockingStartHandler blocks in StartOperation until startGate is closed, which keeps the
// operation in SCHEDULED state, and signals cancelCh when a CancelOperation is received.
func nexusCancelBlockingStartHandler(startGate <-chan struct{}, cancelCh chan struct{}) nexustest.Handler {
	return nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case <-startGate:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "nexus-cancel-deferred"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			select {
			case cancelCh <- struct{}{}:
			default:
			}
			return nil
		},
	}
}

// --- Workflow-backed caller: delivered ------------------------------------------------------------

// Explicit RequestCancelNexusOperation while the caller runs, op STARTED → delivered.
func (s *NexusCancelTestSuite) TestExplicitRequestCancel_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// A signal triggers a workflow task; respond with the cancel command.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRequestCancel(s.T(), env, taskQueue)

	requireCancelDelivered(s.T(), cancelCh)
}

// Explicit cancel while the op is SCHEDULED → deferred; nothing is sent until the op reaches
// STARTED, at which point the cancel fires.
func (s *NexusCancelTestSuite) TestExplicitRequestCancel_Scheduled_DeferredDelivery() {
	cancelCh := make(chan struct{}, 1)
	startGate := make(chan struct{})
	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusCancelBlockingStartHandler(startGate, cancelCh))

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)

	// The handler blocks in StartOperation, so the op stays SCHEDULED.
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED)

	// Requesting cancel now creates a pending cancellation but delivers nothing yet.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRequestCancel(s.T(), env, taskQueue)
	requireCancelNotDelivered(s.T(), cancelCh, time.Second)

	// Releasing the start lets the op reach STARTED, and the deferred cancel fires.
	close(startGate)
	requireCancelDelivered(s.T(), cancelCh)
}

// Caller workflow is cancelled and the workflow honors it (issues the cancel while still running) → delivered.
func (s *NexusCancelTestSuite) TestWorkflowCanceledHonored_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// Cancellation-request produces a workflow task; the workflow "honors" it by cancelling the op.
	s.NoError(env.SdkClient().CancelWorkflow(s.Context(), run.GetID(), run.GetRunID()))
	nexusCancelPollAndRequestCancel(s.T(), env, taskQueue)

	requireCancelDelivered(s.T(), cancelCh)
}

// --- Workflow-backed caller: not delivered (gaps today) -------------------------------------------

// Caller workflow terminated with a STARTED op → not delivered.
func (s *NexusCancelTestSuite) TestTerminateCaller_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test"))
	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
}

// Caller workflow fails with a STARTED op → not delivered.
func (s *NexusCancelTestSuite) TestFailCaller_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRespondClose(s.T(), env, taskQueue, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_FAIL_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_FailWorkflowExecutionCommandAttributes{
			FailWorkflowExecutionCommandAttributes: &commandpb.FailWorkflowExecutionCommandAttributes{
				Failure: &failurepb.Failure{Message: "test"},
			},
		},
	})
	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
}

// Caller workflow completes with a STARTED op still pending → not delivered.
func (s *NexusCancelTestSuite) TestCompleteCaller_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRespondClose(s.T(), env, taskQueue, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{},
		},
	})
	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
}

// Caller workflow hits its run timeout with a STARTED op → not delivered.
func (s *NexusCancelTestSuite) TestRunTimeoutCaller_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 5 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// Run timeout fires around 5s; confirm no cancel is sent as a result.
	requireCancelNotDelivered(s.T(), cancelCh, 10*time.Second)
}

// Caller workflow continues-as-new with a STARTED op → no cancel delivered, and the op is not carried
// into the new run (fresh mutable state drops it).
func (s *NexusCancelTestSuite) TestContinueAsNew_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRespondClose(s.T(), env, taskQueue, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "workflow"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			},
		},
	})
	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
	nexusCancelAwaitNewRunNoPendingOps(s.T(), env, run)
}

// Continue-as-new under REQUEST_CANCEL fires the close policy like any other forced close: the
// operations are not carried into the new run, so the handler is notified. The new run still starts
// with no pending operations.
func (s *NexusCancelTestSuite) TestContinueAsNew_RequestCancelPolicy_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh,
		testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1)) // 1 = REQUEST_CANCEL

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRespondClose(s.T(), env, taskQueue, &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{
				WorkflowType: &commonpb.WorkflowType{Name: "workflow"},
				TaskQueue:    &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			},
		},
	})
	requireCancelDelivered(s.T(), cancelCh)
	nexusCancelAwaitNewRunNoPendingOps(s.T(), env, run)
}

// The operation's own schedule-to-close timeout fires while the caller workflow keeps running → not delivered.
func (s *NexusCancelTestSuite) TestOperationScheduleToCloseTimeout_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 5*time.Second)

	// Wait for the operation to time out on its own (caller workflow stays running).
	await.Require(s.Context(), s.T(), func(c *await.T) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(s.Context(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.Empty(c, resp.PendingNexusOperations, "operation should have timed out")
	}, 20*time.Second, 200*time.Millisecond)

	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
}

// --- Standalone Nexus operations (SANO) — backed by NexusStandaloneTestSuite ----------------------

// Explicit RequestCancelNexusOperationExecution while the op runs → delivered.
func (s *NexusStandaloneTestSuite) TestNexusCancelStandalone_ExplicitCancel_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv()
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: "test-op",
		Endpoint:    endpointName,
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	_, err = env.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: "test-op",
		RunId:       startResp.RunId,
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)

	_, err = env.FrontendClient().RequestCancelNexusOperationExecution(s.Context(), &workflowservice.RequestCancelNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: "test-op",
		RunId:       startResp.RunId,
		Reason:      "test cancellation",
	})
	s.NoError(err)

	requireCancelDelivered(s.T(), cancelCh)
}

// Standalone op terminated → not delivered.
func (s *NexusStandaloneTestSuite) TestNexusCancelStandalone_Terminate_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv()
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: "test-op",
		Endpoint:    endpointName,
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	_, err = env.FrontendClient().PollNexusOperationExecution(s.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: "test-op",
		RunId:       startResp.RunId,
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)

	_, err = env.FrontendClient().TerminateNexusOperationExecution(s.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: "test-op",
		RunId:       startResp.RunId,
		Reason:      "test termination",
	})
	s.NoError(err)

	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)
}

// Standalone op schedule-to-close timeout → not delivered.
func (s *NexusStandaloneTestSuite) TestNexusCancelStandalone_ScheduleToCloseTimeout_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv()
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:            "test-op",
		Endpoint:               endpointName,
		ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	// The operation times out on its own around 4s; confirm no cancel is sent as a result.
	requireCancelNotDelivered(s.T(), cancelCh, 8*time.Second)
}
