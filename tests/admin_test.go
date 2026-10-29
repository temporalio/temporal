package tests

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	chasmnexus "go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/nexus/nexustest"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/tests/testcore"
)

type AdminTestSuite struct {
	parallelsuite.Suite[*AdminTestSuite]
}

type adminRebuildNexusCallback struct {
	url   string
	token string
}

func TestAdminRebuildMutableState_ChasmDisabled(t *testing.T) {
	parallelsuite.Run(t, &AdminTestSuite{}, false)
}

func TestAdminRebuildMutableState_ChasmEnabled(t *testing.T) {
	parallelsuite.Run(t, &AdminTestSuite{}, true)
}

func (s *AdminTestSuite) TestAdminRebuildMutableState(testWithChasm bool) {
	env := s.newRebuildEnv(testWithChasm)

	tv := testvars.New(s.T())
	workflowFn := func(ctx workflow.Context) error {
		var randomUUID string
		err := workflow.SideEffect(
			ctx,
			func(workflow.Context) any { return uuid.New().String() },
		).Get(&randomUUID)
		s.NoError(err)

		_ = workflow.Sleep(ctx, 10*time.Minute)
		return nil
	}

	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := tv.Any().String()
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          env.WorkerTaskQueue(),
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	workflowRun, err := env.SdkClient().ExecuteWorkflow(s.Context(), workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// there are total 6 events, 3 state transitions
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//
	//  3. WorkflowTaskStarted
	//
	//  4. WorkflowTaskCompleted
	//  5. MarkerRecord
	//  6. TimerStarted

	var response1 *adminservice.DescribeMutableStateResponse
	for {
		response1, err = env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: env.Namespace().String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
			Archetype: chasm.WorkflowArchetype,
		})
		s.NoError(err)
		if response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount == 3 {
			// Note: ChasmNodes may be empty even with CHASM enabled, so we only check if the rebuild can be performed,
			// and not checking whether it is rebuildable because ChasmNodes are present.
			if !testWithChasm {
				s.Empty(response1.DatabaseMutableState.ChasmNodes, "CHASM-disabled workflows should not have ChasmNodes")
			}
			break
		}
		time.Sleep(20 * time.Millisecond) //nolint:forbidigo
	}

	_, err = env.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)

	response2, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.VersionHistories, response2.DatabaseMutableState.ExecutionInfo.VersionHistories)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount, response2.DatabaseMutableState.ExecutionInfo.StateTransitionCount)

	s.Equal(response1.DatabaseMutableState.ExecutionState.CreateRequestId, response2.DatabaseMutableState.ExecutionState.CreateRequestId)
	s.Equal(response1.DatabaseMutableState.ExecutionState.RunId, response2.DatabaseMutableState.ExecutionState.RunId)
	s.Equal(response1.DatabaseMutableState.ExecutionState.State, response2.DatabaseMutableState.ExecutionState.State)
	s.Equal(response1.DatabaseMutableState.ExecutionState.Status, response2.DatabaseMutableState.ExecutionState.Status)

	// From transition history perspective, Rebuild is considered as an update to the workflow and updates
	// all sub state machines in the workflow, which includes the workflow ExecutionState.
	s.Equal(&persistencespb.VersionedTransition{
		NamespaceFailoverVersion: response1.DatabaseMutableState.ExecutionState.LastUpdateVersionedTransition.NamespaceFailoverVersion,
		TransitionCount:          response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount + 1,
	}, response2.DatabaseMutableState.ExecutionState.LastUpdateVersionedTransition)

	// Rebuild recreates mutable state for the same run, so the recorded start time must survive it.
	s.NotNil(response1.DatabaseMutableState.ExecutionState.StartTime)
	s.NotNil(response2.DatabaseMutableState.ExecutionState.StartTime)

	timeBefore := timestamp.TimeValue(response1.DatabaseMutableState.ExecutionState.StartTime)
	timeAfter := timestamp.TimeValue(response2.DatabaseMutableState.ExecutionState.StartTime)
	s.Equal(timeBefore, timeAfter)

	s.Equal(
		timestamp.TimeValue(response1.DatabaseMutableState.ExecutionInfo.ExecutionTime),
		timestamp.TimeValue(response2.DatabaseMutableState.ExecutionInfo.ExecutionTime),
	)
	s.Nil(response1.DatabaseMutableState.ExecutionInfo.MutableStateRebuildTime)
	s.NotNil(response2.DatabaseMutableState.ExecutionInfo.MutableStateRebuildTime)
}

func (s *AdminTestSuite) TestAdminRebuildMutableStateRealignsMultipleNexusOperations(testWithChasm bool) {
	if !testWithChasm {
		s.T().Skip("test requires CHASM-backed Nexus operations before the rebuild")
	}

	const operationCount = 3
	const inFlightOperation = "operation-in-flight"
	operationNames := []string{"operation-1", "operation-2", inFlightOperation}
	env := s.newAdminRebuildNexusEnv()
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	releaseStart := make(chan struct{})
	callbackReadyCh := make(chan struct{}, operationCount)
	callbacksByRequestID := make(map[string]adminRebuildNexusCallback, operationCount)
	var callbacksMu sync.Mutex
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
		OnStartOperation: func(
			handlerCtx context.Context,
			_ string,
			operation string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			callbacksMu.Lock()
			if _, seen := callbacksByRequestID[options.RequestID]; !seen {
				callbacksByRequestID[options.RequestID] = adminRebuildNexusCallback{
					url:   options.CallbackURL,
					token: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
				}
				callbackReadyCh <- struct{}{}
			}
			callbacksMu.Unlock()
			if operation == inFlightOperation {
				select {
				case <-releaseStart:
				case <-handlerCtx.Done():
					return nil, handlerCtx.Err()
				}
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: options.RequestID}, nil
		},
	})

	workflowFn := func(ctx workflow.Context) error {
		client := workflow.NewNexusClient(endpointName, "service")
		futures := make([]workflow.NexusOperationFuture, 0, operationCount)
		for _, operation := range operationNames {
			futures = append(futures, client.ExecuteOperation(ctx, operation, "input", workflow.NexusOperationOptions{}))
		}
		for _, future := range futures {
			if err := future.Get(ctx, nil); err != nil {
				return err
			}
		}
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testvars.New(s.T()).Any().String(),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)

	for range operationCount {
		s.Rcv(callbackReadyCh)
	}
	callbacksMu.Lock()
	callbacks := make([]adminRebuildNexusCallback, 0, operationCount)
	for _, callback := range callbacksByRequestID {
		callbacks = append(callbacks, callback)
	}
	callbacksMu.Unlock()
	s.Len(callbacks, operationCount)
	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	assertPendingStates := func(s *AdminTestSuite, expectedStarted, expectedScheduled int) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingNexusOperations, operationCount)
		var started, scheduled int
		for _, operation := range desc.PendingNexusOperations {
			switch operation.State {
			case enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED:
				started++
			case enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED:
				scheduled++
			default:
				continue
			}
		}
		s.Equal(expectedStarted, started)
		s.Equal(expectedScheduled, scheduled)
	}
	s.Await(func(s *AdminTestSuite) {
		history := env.GetHistory(env.Namespace().String(), execution)
		var scheduled, started int
		for _, event := range history {
			switch event.GetEventType() {
			case enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED:
				scheduled++
			case enumspb.EVENT_TYPE_NEXUS_OPERATION_STARTED:
				started++
			default:
				continue
			}
		}
		s.Equal(operationCount, scheduled)
		s.Equal(operationCount-1, started)
		assertPendingStates(s, operationCount-1, 1)
	}, 10*time.Second, 50*time.Millisecond)
	s.Len(s.assertAdminRebuildNexusOperationTrees(ctx, env, execution, false), operationCount)

	s.rebuildAdminNexusOperationToHSM(ctx, env, execution)
	s.Len(s.assertAdminRebuildNexusOperationTrees(ctx, env, execution, true), operationCount)
	assertPendingStates(s, operationCount-1, 1)
	close(releaseStart)
	s.Await(func(s *AdminTestSuite) {
		assertPendingStates(s, operationCount, 0)
	}, 10*time.Second, 50*time.Millisecond)
	for _, callback := range callbacks {
		s.sendAdminRebuildNexusCompletion(ctx, callback, nexusrpc.CompleteOperationOptions{
			Result: testcore.MustToPayload(s.T(), "result"),
		})
	}
	s.NoError(run.Get(ctx, nil))
}

func (s *AdminTestSuite) TestAdminRebuildMutableStateRealignsNexusOperationAfterCancellationRequest(testWithChasm bool) {
	if !testWithChasm {
		s.T().Skip("test requires a CHASM-backed Nexus operation before the rebuild")
	}
	s.testAdminRebuildMutableStateRealignsNexusCancellation(false)
}

func (s *AdminTestSuite) TestAdminRebuildMutableStateRealignsNexusCancellationInFlight(testWithChasm bool) {
	if !testWithChasm {
		s.T().Skip("test requires a CHASM-backed Nexus operation before the rebuild")
	}
	s.testAdminRebuildMutableStateRealignsNexusCancellation(true)
}

func (s *AdminTestSuite) testAdminRebuildMutableStateRealignsNexusCancellation(cancelInFlight bool) {
	s.T().Helper()

	env := s.newAdminRebuildNexusEnv()
	ctx, cancel := context.WithTimeout(s.Context(), 30*time.Second)
	defer cancel()

	callbackInfoCh := make(chan adminRebuildNexusCallback, 1)
	cancelHandled := make(chan struct{}, 1)
	releaseCancel := make(chan struct{})
	var cancelCalls atomic.Int32
	endpointName := env.createRandomExternalNexusServer(ctx, s.T(), nexustest.Handler{
		OnStartOperation: func(
			_ context.Context,
			_ string,
			_ string,
			_ *nexus.LazyValue,
			options nexus.StartOperationOptions,
		) (nexus.HandlerStartOperationResult[any], error) {
			select {
			case callbackInfoCh <- adminRebuildNexusCallback{
				url:   options.CallbackURL,
				token: options.CallbackHeader.Get(commonnexus.CallbackTokenHeader),
			}:
			default:
			}
			return &nexus.HandlerStartOperationResultAsync{OperationToken: "test-operation-token"}, nil
		},
		OnCancelOperation: func(
			handlerCtx context.Context,
			_ string,
			_ string,
			_ string,
			_ nexus.CancelOperationOptions,
		) error {
			cancelCalls.Add(1)
			select {
			case cancelHandled <- struct{}{}:
			default:
			}
			if !cancelInFlight {
				return nil
			}
			select {
			case <-releaseCancel:
				return nil
			case <-handlerCtx.Done():
				return handlerCtx.Err()
			}
		},
	})

	const awaitStartedUpdate = "await-started"
	workflowFn := func(ctx workflow.Context) error {
		started := false
		if err := workflow.SetUpdateHandler(ctx, awaitStartedUpdate, func(ctx workflow.Context) error {
			return workflow.Await(ctx, func() bool { return started })
		}); err != nil {
			return err
		}

		client := workflow.NewNexusClient(endpointName, "service")
		operationCtx, cancelOperation := workflow.WithCancel(ctx)
		future := client.ExecuteOperation(operationCtx, "operation", "input", workflow.NexusOperationOptions{})
		var execution workflow.NexusOperationExecution
		if err := future.GetNexusOperationExecution().Get(ctx, &execution); err != nil {
			return err
		}
		started = true

		workflow.GetSignalChannel(ctx, "cancel").Receive(ctx, nil)
		cancelOperation()
		_ = future.Get(ctx, nil)
		workflow.GetSignalChannel(ctx, "finish").Receive(ctx, nil)
		return nil
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        testvars.New(s.T()).Any().String(),
		TaskQueue: env.WorkerTaskQueue(),
	}, workflowFn)
	s.NoError(err)
	startedHandle, err := env.SdkClient().UpdateWorkflow(ctx, sdkclient.UpdateWorkflowOptions{
		WorkflowID:   run.GetID(),
		RunID:        run.GetRunID(),
		UpdateName:   awaitStartedUpdate,
		WaitForStage: sdkclient.WorkflowUpdateStageCompleted,
	})
	s.NoError(err)
	s.NoError(startedHandle.Get(ctx, nil))
	callback := s.Rcv(callbackInfoCh)
	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "cancel", nil))
	s.Rcv(cancelHandled)
	s.Equal(int32(1), cancelCalls.Load())

	execution := &commonpb.WorkflowExecution{WorkflowId: run.GetID(), RunId: run.GetRunID()}
	if cancelInFlight {
		history := env.GetHistory(env.Namespace().String(), execution)
		s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED)
		s.RequireNoHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED)
	} else {
		s.Await(func(s *AdminTestSuite) {
			history := env.GetHistory(env.Namespace().String(), execution)
			s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUESTED)
			s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED)
		}, 10*time.Second, 50*time.Millisecond)
	}
	s.assertAdminRebuildNexusOperationTree(ctx, env, execution, false)

	s.rebuildAdminNexusOperationToHSM(ctx, env, execution)
	s.assertAdminRebuildNexusOperationTree(ctx, env, execution, true)
	if cancelInFlight {
		close(releaseCancel)
		s.Await(func(s *AdminTestSuite) {
			history := env.GetHistory(env.Namespace().String(), execution)
			s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCEL_REQUEST_COMPLETED)
		}, 10*time.Second, 50*time.Millisecond)
	} else {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, run.GetID(), run.GetRunID())
		s.NoError(err)
		s.Len(desc.PendingNexusOperations, 1)
		s.Equal(
			enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED,
			desc.PendingNexusOperations[0].GetCancellationInfo().GetState(),
		)
	}
	// The cancellation reconstructed in HSM must resolve before the original CHASM completion token is used for the
	// terminal callback.
	s.sendAdminRebuildNexusCompletion(ctx, callback, nexusrpc.CompleteOperationOptions{
		Error: nexus.NewOperationCanceledErrorf("operation canceled"),
	})
	s.Await(func(s *AdminTestSuite) {
		history := env.GetHistory(env.Namespace().String(), execution)
		s.RequireHistoryEvent(history, enumspb.EVENT_TYPE_NEXUS_OPERATION_CANCELED)
	}, 10*time.Second, 50*time.Millisecond)

	s.NoError(env.SdkClient().SignalWorkflow(ctx, run.GetID(), run.GetRunID(), "finish", nil))
	s.NoError(run.Get(ctx, nil))
	if !cancelInFlight {
		s.Equal(int32(1), cancelCalls.Load())
	}
}

func (s *AdminTestSuite) newAdminRebuildNexusEnv() *NexusTestEnv {
	return newNexusTestEnv(
		s.T(),
		true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(chasmnexus.Enabled, true),
		testcore.WithDynamicConfig(chasmnexus.EnableChasmWorkflowOperations, true),
		testcore.WithDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, 100),
	)
}

func (s *AdminTestSuite) rebuildAdminNexusOperationToHSM(
	ctx context.Context,
	env *NexusTestEnv,
	execution *commonpb.WorkflowExecution,
) {
	s.T().Helper()
	// Keep the CHASM reader enabled but stop routing rebuilt operations to CHASM.
	env.OverrideDynamicConfig(chasmnexus.ChasmWorkflowOperationsRolloutPercent, 0)
	_, err := env.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
	})
	s.NoError(err)
}

func (s *AdminTestSuite) sendAdminRebuildNexusCompletion(
	ctx context.Context,
	callback adminRebuildNexusCallback,
	completion nexusrpc.CompleteOperationOptions,
) {
	s.T().Helper()
	completion.Header = nexus.Header{commonnexus.CallbackTokenHeader: callback.token}
	completionClient := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: commonnexus.PayloadSerializer,
	})
	s.NoError(completionClient.CompleteOperation(ctx, callback.url, completion))
}

// assertAdminRebuildNexusOperationTree asserts which tree contains the single pending operation.
func (s *AdminTestSuite) assertAdminRebuildNexusOperationTree(
	ctx context.Context,
	env *NexusTestEnv,
	execution *commonpb.WorkflowExecution,
	expectHSM bool,
) {
	s.T().Helper()
	s.Len(s.assertAdminRebuildNexusOperationTrees(ctx, env, execution, expectHSM), 1)
}

func (s *AdminTestSuite) assertAdminRebuildNexusOperationTrees(
	ctx context.Context,
	env *NexusTestEnv,
	execution *commonpb.WorkflowExecution,
	expectHSM bool,
) []string {
	s.T().Helper()
	history := env.GetHistory(env.Namespace().String(), execution)
	var opIDs []string
	for _, event := range history {
		if event.GetEventType() == enumspb.EVENT_TYPE_NEXUS_OPERATION_SCHEDULED {
			opIDs = append(opIDs, strconv.FormatInt(event.EventId, 10))
		}
	}
	s.NotEmpty(opIDs)

	desc, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: execution,
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	for _, opID := range opIDs {
		_, inHSM := desc.DatabaseMutableState.GetExecutionInfo().
			GetSubStateMachinesByType()[nexusoperations.OperationMachineType].GetMachinesById()[opID]
		_, inCHASM := desc.DatabaseMutableState.GetChasmNodes()["Operations#"+opID]
		s.Equal(expectHSM, inHSM, "unexpected HSM presence for operation %s", opID)
		s.Equal(!expectHSM, inCHASM, "unexpected CHASM presence for operation %s", opID)
	}
	return opIDs
}

// TestAdminRebuildMutableStateRunTimeout checks that the rebuild re-anchors the run timeout
// deadline at the rebuild time, the way reset does, and that the run still times out there.
func (s *AdminTestSuite) TestAdminRebuildMutableStateRunTimeout(testWithChasm bool) {
	const runTimeout = 10 * time.Second

	env := s.newRebuildEnv(testWithChasm)
	ctx, cancel := context.WithTimeout(s.Context(), 90*time.Second)
	defer cancel()

	workflowFn := func(ctx workflow.Context) error {
		return workflow.Sleep(ctx, time.Hour)
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := testvars.New(s.T()).Any().String()
	_, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: env.WorkerTaskQueue(),
		// No execution timeout, so the run deadline is purely start time plus run timeout.
		WorkflowRunTimeout: runTimeout,
	}, workflowFn)
	s.NoError(err)

	before := s.awaitRebuildableMutableState(ctx, env, workflowID)
	originalRunExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowRunExpirationTime)
	s.False(originalRunExpiration.IsZero())
	s.Zero(timestamp.TimeValue(before.ExecutionInfo.WorkflowExecutionExpirationTime))

	s.rebuildMutableState(ctx, env, workflowID)

	after := s.describeMutableState(ctx, env, workflowID)
	runExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowRunExpirationTime)
	s.True(runExpiration.After(originalRunExpiration))
	s.NotNil(after.ExecutionInfo.MutableStateRebuildTime)
	s.Zero(
		timestamp.TimeValue(after.ExecutionInfo.WorkflowExecutionExpirationTime),
	)

	s.assertTimesOutAt(ctx, env, workflowID, runExpiration)
}

// TestAdminRebuildMutableStateExecutionTimeout checks that the rebuild re-anchors the execution
// timeout deadline at the rebuild time and that the workflow still times out there, the same thing
// TestAdminRebuildMutableStateRunTimeout checks for the run timeout deadline.
//
// The continue-as-new is what puts the execution timeout timer under test. The server skips the
// WorkflowExecutionTimeoutTask on a chain's first run and lets the run
// timeout timer enforce the deadline there, so a first run would exercise the same path as
// TestAdminRebuildMutableStateRunTimeout. On the second run the opposite holds: the run timeout
// defaults to the execution timeout, which makes both deadlines the same instant, and
// GenerateWorkflowStartTasks then skips the run timeout task, leaving the execution timeout timer
// as the only thing that can end the workflow.
func (s *AdminTestSuite) TestAdminRebuildMutableStateExecutionTimeout(testWithChasm bool) {
	const executionTimeout = 15 * time.Second

	env := s.newRebuildEnv(testWithChasm)
	ctx, cancel := context.WithTimeout(s.Context(), 90*time.Second)
	defer cancel()

	var workflowFn func(ctx workflow.Context) error
	workflowFn = func(ctx workflow.Context) error {
		if workflow.GetInfo(ctx).ContinuedExecutionRunID == "" {
			return workflow.NewContinueAsNewError(ctx, workflowFn)
		}
		return workflow.Sleep(ctx, time.Hour)
	}
	env.SdkWorker().RegisterWorkflow(workflowFn)

	workflowID := testvars.New(s.T()).Any().String()
	run, err := env.SdkClient().ExecuteWorkflow(ctx, sdkclient.StartWorkflowOptions{
		ID:                       workflowID,
		TaskQueue:                env.WorkerTaskQueue(),
		WorkflowExecutionTimeout: executionTimeout,
	}, workflowFn)
	s.NoError(err)
	firstRunID := run.GetRunID()

	s.Await(func(s *AdminTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
		s.NoError(err)
		s.NotEqual(firstRunID, desc.GetWorkflowExecutionInfo().GetExecution().GetRunId(),
			"workflow should continue as new")
	}, 20*time.Second, 100*time.Millisecond)

	before := s.awaitRebuildableMutableState(ctx, env, workflowID)
	originalExecutionExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowExecutionExpirationTime)
	originalRunExpiration := timestamp.TimeValue(before.ExecutionInfo.WorkflowRunExpirationTime)
	s.False(originalExecutionExpiration.IsZero(), "execution timeout deadline should be set before the rebuild")
	s.NotZero(before.ExecutionInfo.WorkflowExecutionTimerTaskStatus,
		"the second run should carry an execution timeout timer task")

	s.rebuildMutableState(ctx, env, workflowID)

	after := s.describeMutableState(ctx, env, workflowID)
	executionExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowExecutionExpirationTime)
	runExpiration := timestamp.TimeValue(after.ExecutionInfo.WorkflowRunExpirationTime)
	s.True(executionExpiration.After(originalExecutionExpiration),
		"rebuild should re-anchor the execution deadline")
	s.True(runExpiration.After(originalRunExpiration), "rebuild should re-anchor the run deadline")
	s.NotZero(after.ExecutionInfo.WorkflowExecutionTimerTaskStatus,
		"the execution timeout timer task should survive the rebuild")

	s.assertTimesOutAt(ctx, env, workflowID, executionExpiration)
}

func (s *AdminTestSuite) newRebuildEnv(testWithChasm bool) *testcore.TestEnv {
	env := testcore.NewEnv(s.T(), testcore.WithDynamicConfig(dynamicconfig.EnableChasm, testWithChasm))

	if testWithChasm {
		configValues := env.GetTestCluster().Host().DcClient().GetValue(dynamicconfig.EnableChasm.Key())
		s.NotEmpty(configValues, "EnableChasm config should be set")
		configValue, _ := configValues[0].Value.(bool)
		s.True(configValue, "EnableChasm config should be true")
	}
	return env
}

func (s *AdminTestSuite) describeMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) *persistencespb.WorkflowMutableState {
	resp, err := env.AdminClient().DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
		Archetype: chasm.WorkflowArchetype,
	})
	s.NoError(err)
	return resp.DatabaseMutableState
}

// awaitRebuildableMutableState waits until the current run has completed its first workflow task,
// so that the rebuild replays more than just the start event.
func (s *AdminTestSuite) awaitRebuildableMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) *persistencespb.WorkflowMutableState {
	var mutableState *persistencespb.WorkflowMutableState
	s.Await(func(s *AdminTestSuite) {
		mutableState = s.describeMutableState(ctx, env, workflowID)
		s.Positive(mutableState.ExecutionInfo.LastCompletedWorkflowTaskStartedEventId,
			"workflow should complete its first workflow task")
	}, 20*time.Second, 50*time.Millisecond)
	return mutableState
}

func (s *AdminTestSuite) rebuildMutableState(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
) {
	_, err := env.AdminClient().RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: env.Namespace().String(),
		Execution: &commonpb.WorkflowExecution{WorkflowId: workflowID},
	})
	s.NoError(err)
}

// assertTimesOutAt asserts that the workflow survives the rebuild and then times out at deadline.
func (s *AdminTestSuite) assertTimesOutAt(
	ctx context.Context,
	env *testcore.TestEnv,
	workflowID string,
	deadline time.Time,
) {
	desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
	s.NoError(err)
	s.NotEqual(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.GetWorkflowExecutionInfo().GetStatus(),
		"workflow should not time out immediately after the rebuild")

	s.Await(func(s *AdminTestSuite) {
		desc, err := env.SdkClient().DescribeWorkflowExecution(ctx, workflowID, "")
		s.NoError(err)
		s.Equal(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT, desc.GetWorkflowExecutionInfo().GetStatus(),
			"workflow did not time out at its deadline")
	}, time.Until(deadline)+5*time.Second, 250*time.Millisecond)
}
