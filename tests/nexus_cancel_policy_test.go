package tests

import (
	"testing"
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
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
		testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1), // 1 = REQUEST_CANCEL
	)...)
}

func (s *NexusCancelPolicyTestSuite) nexusCancelEnv(cancelCh chan struct{}) (*NexusTestEnv, string, string) {
	env := s.newTestEnv()
	taskQueue := testcore.RandomizeStr(s.T().Name())
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), nexusClosePolicyHandler(cancelCh))
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

	s.NoError(env.SdkClient().SignalWorkflow(env.Context(), run.GetID(), run.GetRunID(), "close", nil))
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

	s.NoError(env.SdkClient().TerminateWorkflow(env.Context(), run.GetID(), run.GetRunID(), "test"))
	requireCancelDelivered(s.T(), cancelCh)
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

	s.NoError(env.SdkClient().SignalWorkflow(env.Context(), run.GetID(), run.GetRunID(), "close", nil))
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

	s.NoError(env.SdkClient().SignalWorkflow(env.Context(), run.GetID(), run.GetRunID(), "close", nil))
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

// --- Still not delivered (intentional or not-yet-implemented under REQUEST_CANCEL) ----------------

// Continue-as-new does NOT fire the policy even under REQUEST_CANCEL — the new run inherits the
// operations and applies the policy on its own close (intentional; CaN is not hooked).
func (s *NexusCancelPolicyTestSuite) TestContinueAsNew_StillNotDelivered() {
	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 0)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	s.NoError(env.SdkClient().SignalWorkflow(env.Context(), run.GetID(), run.GetRunID(), "close", nil))
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

// The operation's own schedule-to-close timeout (caller workflow stays running). The cancel is now
// scheduled (the STC-timeout handler calls RequestCancelOnAutoClose), but the schedule-to-close
// timeout is itself a timer/pure task, so it hits the same timer-driven dispatch bug: the outbound
// cancel task is scheduled but not dispatched.
func (s *NexusCancelPolicyTestSuite) TestOperationScheduleToCloseTimeout_Delivered() {
	s.T().Skip("KNOWN LIMITATION: when a workflow-backed operation hits its own schedule-to-close timeout, the " +
		"timeout resolution removes the operation from the caller workflow's pending-operations map in the same CHASM " +
		"transaction. The auto-close Cancellation is created as a child of that operation, so its node is never " +
		"materialized (syncSubField is skipped for a to-be-deleted node) and the outbound CancelOperation task is " +
		"dropped. WithDetached only governs access after parent close, not structural survival when the parent is " +
		"removed. Delivering here requires either deferring the operation's timeout resolution until the cancel " +
		"round-trip completes, or relocating the auto-close cancellation off the operation onto a longer-lived parent. " +
		"Workflow-close (terminate/fail/complete/cancel/run-timeout/execution-timeout) and SANO timeout keep the " +
		"operation alive (or use the closed-entity snapshot path) and do deliver.")

	cancelCh := make(chan struct{}, 1)
	env, taskQueue, endpointName := s.nexusCancelEnv(cancelCh)

	run := nexusCancelStartSchedule(s.T(), env, endpointName, client.StartWorkflowOptions{
		TaskQueue:           taskQueue,
		WorkflowTaskTimeout: 10 * time.Second,
	}, 5*time.Second)
	nexusCancelAwaitOpState(s.T(), env, run, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED)

	requireCancelDelivered(s.T(), cancelCh)
}

// --- Standalone (SANO) under REQUEST_CANCEL -------------------------------------------------------

// Standalone op terminated — per the design this should notify the handler.
func (s *NexusStandaloneTestSuite) TestNexusCancelPolicyStandalone_Terminate_Delivered() {
	cancelCh := make(chan struct{}, 1)
	env := s.newTestEnv(testcore.WithDynamicConfig(dynamicconfig.NexusOperationAutoClosePolicy, 1))
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId: "test-op",
		Endpoint:    endpointName,
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	_, err = env.FrontendClient().PollNexusOperationExecution(env.Context(), &workflowservice.PollNexusOperationExecutionRequest{
		Namespace:   env.Namespace().String(),
		OperationId: "test-op",
		RunId:       startResp.RunId,
		WaitStage:   enumspb.NEXUS_OPERATION_WAIT_STAGE_STARTED,
	})
	s.NoError(err)

	_, err = env.FrontendClient().TerminateNexusOperationExecution(env.Context(), &workflowservice.TerminateNexusOperationExecutionRequest{
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
	endpointName := env.createRandomExternalNexusServer(env.Context(), s.T(), nexusClosePolicyHandler(cancelCh))

	startResp, err := s.startNexusOperation(env, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:            "test-op",
		Endpoint:               endpointName,
		ScheduleToCloseTimeout: durationpb.New(4 * time.Second),
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	requireCancelDelivered(s.T(), cancelCh)
}
