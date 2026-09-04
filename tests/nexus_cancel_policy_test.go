package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

// NexusCancelPolicyTestSuite is the REQUEST_CANCEL counterpart to NexusCancelTestSuite: it runs the
// same scenarios but with the close policy set to REQUEST_CANCEL, and verifies that the close-driven
// cases that were "not delivered" today now deliver a CancelOperation to the handler.
//
// What delivers: synchronous closes (terminate/fail/complete/cancel), standalone terminate, and the
// timeout paths (workflow run/execution timeout, standalone schedule-to-close timeout) — the latter
// via the auto_close flag on the cancellation, which skips the cancel-call clamp to the operation's
// (~expired) remaining schedule-to-close time.
// What does not: continue-as-new is intentionally not hooked; the workflow-backed operation's own
// schedule-to-close timeout still does not dispatch (a separate pure-task-on-a-running-workflow
// dispatch issue — see the skipped test).
type NexusCancelPolicyTestSuite struct {
	parallelsuite.Suite[*NexusCancelPolicyTestSuite]
}

func TestNexusCancelPolicyTestSuite(t *testing.T) {
	parallelsuite.Run(t, &NexusCancelPolicyTestSuite{})
}

func (s *NexusCancelPolicyTestSuite) newTestEnv(opts ...testcore.TestOption) *NexusTestEnv {
	return newNexusTestEnv(s.T(), true, append(
		opts,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, 100),
		testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1), // 1 = REQUEST_CANCEL
	)...)
}

func (s *NexusCancelPolicyTestSuite) nexusCancelEnv(cancelCh chan struct{}) (env *NexusTestEnv, taskQueue, endpointName string) {
	env = s.newTestEnv()
	taskQueue = testcore.RandomizeStr(s.T().Name())
	endpointName = env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))
	return env, taskQueue, endpointName
}

// --- Delivered (unchanged from baseline) ----------------------------------------------------------

func (s *NexusCancelPolicyTestSuite) TestExplicitRequestCancel_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "close", nil))
	nexusCancelPollAndRequestCancel(s.T(), env, taskQueue)

	requireCancelDelivered(s.T(), cancelCh)
}

// --- Now delivered under REQUEST_CANCEL (flipped from baseline) ------------------------------------

// Caller workflow terminated with a STARTED op → now delivered.
func (s *NexusCancelPolicyTestSuite) TestTerminateCaller_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test"))
	requireCancelDelivered(s.T(), cancelCh)
}

// Caller workflow terminated while the operation's start is still in-flight (handler blocked in
// StartOperation, op SCHEDULED). The caller never records a token, so even under REQUEST_CANCEL no
// CancelOperation can be delivered — the operation is abandoned. Pins the in-flight-start orphan:
// REQUEST_CANCEL guarantees delivery only for operations the caller durably observed as STARTED.
func (s *NexusCancelPolicyTestSuite) TestTerminateCaller_StartInFlight_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	startGate := make(chan struct{})
	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusCancelBlockingStartHandler(startGate, cancelCh))

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	// The handler blocks in StartOperation, so the start is in-flight and the op stays SCHEDULED.
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED)

	// Terminate the caller while the start is still in-flight.
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test"))

	// Releasing the start lets the handler "start", but the caller is already closed and never
	// recorded the token, so the operation can never reach STARTED and no cancel is delivered.
	close(startGate)
	requireCancelNotDelivered(s.T(), cancelCh, 5*time.Second)
}

// Caller workflow fails with a STARTED op → now delivered.
func (s *NexusCancelPolicyTestSuite) TestFailCaller_Delivered() {
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
	requireCancelDelivered(s.T(), cancelCh)
}

// Caller workflow completes with a STARTED op still pending → now delivered.
func (s *NexusCancelPolicyTestSuite) TestCompleteCaller_Delivered() {
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
	requireCancelDelivered(s.T(), cancelCh)
}

// Caller workflow hits its run timeout with a STARTED op → delivered.
//
// The cancel fires at the operation's (~expired) schedule-to-close deadline. The auto_close flag on
// the cancellation skips the cancel-call clamp to the remaining schedule-to-close time (which would
// otherwise starve the call below MinRequestTimeout), while user-initiated cancels keep the clamp.
func (s *NexusCancelPolicyTestSuite) TestRunTimeoutCaller_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:          taskQueue,
		WorkflowRunTimeout: 5 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	requireCancelDelivered(s.T(), cancelCh)
}

// Caller workflow hits its execution timeout with a STARTED op → delivered (auto_close skips the
// cancel-call clamp, same as TestRunTimeoutCaller_Delivered).
func (s *NexusCancelPolicyTestSuite) TestExecutionTimeoutCaller_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:                taskQueue,
		WorkflowExecutionTimeout: 5 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	requireCancelDelivered(s.T(), cancelCh)
}

// Continue-as-new abandons the caller's pending operations (they are not carried into the new run),
// so under REQUEST_CANCEL it fires the policy like any other forced close and notifies the handler.
func (s *NexusCancelPolicyTestSuite) TestContinueAsNew_Delivered() {
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
	requireCancelDelivered(s.T(), cancelCh)
	nexusCancelAwaitNewRunNoPendingOps(s.T(), env, run)
}

// Case [D]: the operation's own schedule-to-close timeout fires while the caller workflow keeps
// running. The timeout resolution event-sources a cancel request and defers removing the operation
// until the cancel is delivered, so the handler is notified. Once the cancel completes the operation
// is removed (no longer pending on the still-running caller).
func (s *NexusCancelPolicyTestSuite) TestOperationScheduleToCloseTimeout_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 5*time.Second)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	requireCancelDelivered(s.T(), cancelCh)

	// After the cancel is delivered the timed-out operation is removed from the still-running caller.
	await.Require(s.Context(), s.T(), func(c *await.T) {
		resp, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.Empty(c, resp.PendingNexusOperations, "timed-out operation should be removed after the cancel is delivered")
	}, 20*time.Second, 200*time.Millisecond)
}

// The operation's start-to-close timeout is the sibling of schedule-to-close: both close the
// operation while the caller keeps running, so both must notify the handler.
func (s *NexusCancelPolicyTestSuite) TestOperationStartToCloseTimeout_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0, func(a *commandpb.ScheduleNexusOperationCommandAttributes) {
		a.StartToCloseTimeout = durationpb.New(5 * time.Second)
	})
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	requireCancelDelivered(s.T(), cancelCh)
}

// --- Workflow reset ------------------------------------------------------------------------------

// Reset to a point before the operation was scheduled. NexusOperationScheduled is never
// cherry-picked, so the reset run never rebuilds the operation and its handler is left with no
// caller — the same orphan the policy exists to clean up, so the cancel is delivered.
func (s *NexusCancelPolicyTestSuite) TestResetDropsOperation_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// The first WorkflowTaskCompleted is the one that scheduled the operation, so rebuilding through
	// its predecessor drops the operation.
	nexusCancelReset(s.T(), env, run, nexusCancelWFTCompletedEventID(s.T(), env, run, true))

	requireCancelDelivered(s.T(), cancelCh)
}

// Reset to a point after the operation started. The reset run rebuilds it with the same request ID
// and operation token (the handler's callback token still resolves to the new run), so the handler
// is not orphaned and the policy must not fire.
func (s *NexusCancelPolicyTestSuite) TestResetAdoptsOperation_NotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// NexusOperationStarted schedules a workflow task; completing it with no commands gives a reset
	// point that sits after the operation started.
	nexusCancelPollAndRespondEmpty(s.T(), env, taskQueue)
	nexusCancelReset(s.T(), env, run, nexusCancelWFTCompletedEventID(s.T(), env, run, false))

	requireCancelNotDelivered(s.T(), cancelCh, 3*time.Second)

	// The operation is carried into the reset run rather than abandoned.
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)
}

// Reset after the cancel already took effect on the handler. The caller is terminated under
// REQUEST_CANCEL, the handler honors the cancel and reports the outcome — but the caller run is already
// closed, so that outcome has nowhere to land. Resetting to a point after the operation started then
// re-adopts the operation on the new run, which must not be left waiting for an outcome that already
// happened.
func (s *NexusCancelPolicyTestSuite) TestResetAfterCancelCompleted_AdoptedOperationResolves() {
	cancelCh := make(chan struct{}, 1)
	var callbackToken, publicCallbackURL string

	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "nexus-cancel-reset"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			select {
			case cancelCh <- struct{}{}:
			default:
			}
			return nil
		},
	})

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// NexusOperationStarted schedules a workflow task; completing it gives a reset point that sits
	// after the operation started, so the reset run adopts the operation.
	nexusCancelPollAndRespondEmpty(s.T(), env, taskQueue)
	resetPoint := nexusCancelWFTCompletedEventID(s.T(), env, run, false)

	// Terminate: the close policy requests the cancel and the handler receives it.
	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test"))
	requireCancelDelivered(s.T(), cancelCh)

	// The handler honors the cancel and reports the canceled outcome. The caller run is already closed,
	// so this completion has no open history to land in.
	completionErr := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	}).CompleteOperation(s.Context(), publicCallbackURL, nexusrpc.CompleteOperationOptions{
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
		Error: &nexus.OperationError{
			State: nexus.OperationStateCanceled,
			Cause: errors.New("canceled by handler"),
		},
	})
	s.T().Logf("completion delivered to the closed run returned: %v", completionErr)

	// Reset to after the operation started: the new run re-adopts the operation.
	nexusCancelReset(s.T(), env, run, resetPoint)

	// The adopted operation must not stay pending forever — its outcome already happened handler-side.
	await.Require(s.T().Context(), s.T(), func(c *await.T) {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.Empty(c, desc.PendingNexusOperations,
			"reset run is still waiting on an operation the handler already canceled")
	}, 15*time.Second, 500*time.Millisecond)
}

// A completion delivered *after* the reset must reach the new run: the callback targets the
// superseded run, and run fallback re-targets it to the run that now owns the operation. This is the
// mirror of TestResetAfterCancelCompleted_AdoptedOperationResolves, where the completion arrives
// before the new run exists and is lost. It guards against a change that lets a closed run absorb
// completions, which would starve the run actually waiting for the outcome.
func (s *NexusCancelPolicyTestSuite) TestCompletionAfterReset_ResolvesOnNewRun() {
	cancelCh := make(chan struct{}, 1)
	var callbackToken, publicCallbackURL string

	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			callbackToken = options.CallbackHeader.Get(commonnexus.CallbackTokenHeader)
			publicCallbackURL = options.CallbackURL
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "nexus-completion-after-reset"}, nil
		},
		OnCancelOperation: func(ctx context.Context, service, operation, token string, options nexus.CancelOperationOptions) error {
			select {
			case cancelCh <- struct{}{}:
			default:
			}
			return nil
		},
	})

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	nexusCancelPollAndRespondEmpty(s.T(), env, taskQueue)
	resetPoint := nexusCancelWFTCompletedEventID(s.T(), env, run, false)

	s.NoError(env.SdkClient().TerminateWorkflow(s.Context(), run.GetID(), run.GetRunID(), "test"))
	requireCancelDelivered(s.T(), cancelCh)

	// Reset first, so the new run exists and is the one waiting for the outcome.
	nexusCancelReset(s.T(), env, run, resetPoint)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	// Only now does the handler report the outcome, against the superseded run's callback URL.
	s.NoError(nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	}).CompleteOperation(s.Context(), publicCallbackURL, nexusrpc.CompleteOperationOptions{
		Header: nexus.Header{commonnexus.CallbackTokenHeader: callbackToken},
		Error: &nexus.OperationError{
			State: nexus.OperationStateCanceled,
			Cause: errors.New("canceled by handler"),
		},
	}))

	// The adopted operation on the new run resolves rather than hanging.
	await.Require(s.T().Context(), s.T(), func(c *await.T) {
		desc, err := env.FrontendClient().DescribeWorkflowExecution(testcore.NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{WorkflowId: run.GetID()},
		})
		require.NoError(c, err)
		require.Empty(c, desc.PendingNexusOperations,
			"completion delivered after the reset should resolve the operation on the new run")
	}, 15*time.Second, 500*time.Millisecond)
}

// Reset to a point after the operation was scheduled but before it started. NexusOperationScheduled
// replays (so the operation is carried over) while Started does not, leaving it back in "scheduled"
// and re-invoked — with the same request ID, so a handler that deduplicates reconnects to the
// operation it already started rather than starting a second one.
func (s *NexusCancelPolicyTestSuite) TestResetBeforeStarted_ReinvokedWithSameRequestID() {
	startGate := make(chan struct{})
	var mu sync.Mutex
	var startRequestIDs []string

	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexustest.Handler{
		OnStartOperation: func(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
			mu.Lock()
			startRequestIDs = append(startRequestIDs, options.RequestID)
			first := len(startRequestIDs) == 1
			mu.Unlock()
			if first {
				// Hold the first attempt so a workflow task can complete while the operation is still
				// "scheduled", giving us a reset point between Scheduled and Started.
				select {
				case <-startGate:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "nexus-reset-before-started"}, nil
		},
	})

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED)

	// Signal produces a workflow task while the operation is still scheduled; completing it gives a
	// reset point that sits after Scheduled but before Started.
	s.NoError(env.SdkClient().SignalWorkflow(s.Context(), run.GetID(), run.GetRunID(), "pause", nil))
	nexusCancelPollAndRespondEmpty(s.T(), env, taskQueue)
	resetPoint := nexusCancelWFTCompletedEventID(s.T(), env, run, false)

	// Let the operation start, so Started lands after the reset point.
	close(startGate)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	nexusCancelReset(s.T(), env, run, resetPoint)

	// The reset run re-invokes the start, reusing the original request ID.
	await.Require(s.T().Context(), s.T(), func(c *await.T) {
		mu.Lock()
		defer mu.Unlock()
		require.GreaterOrEqual(c, len(startRequestIDs), 2, "expected the reset run to re-invoke StartOperation")
		require.NotEmpty(c, startRequestIDs[0], "request ID must be set, otherwise the reuse check below is vacuous")
		require.Equal(c, startRequestIDs[0], startRequestIDs[len(startRequestIDs)-1],
			"re-invocation must reuse the original request ID so the handler can deduplicate")
	}, 20*time.Second, 200*time.Millisecond)
}

// --- Standalone (SANO) under REQUEST_CANCEL -------------------------------------------------------

// Standalone op terminated — per the design this should notify the handler.
func (s *NexusStandaloneTestSuite) TestNexusCancelPolicyStandalone_Terminate_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1))
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

	requireCancelDelivered(s.T(), cancelCh)
}

// Standalone op schedule-to-close timeout — hits the same cancel-call-timeout clamp as
// TestRunTimeoutCaller_Delivered (the cancel fires at the expired schedule-to-close deadline).
func (s *NexusStandaloneTestSuite) TestNexusCancelPolicyStandalone_ScheduleToCloseTimeout_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1))
	endpointName := env.createRandomExternalNexusServer(s.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:            "test-op",
		Endpoint:               endpointName,
		ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	requireCancelDelivered(s.T(), cancelCh)
}
