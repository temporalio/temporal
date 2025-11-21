package tests

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/components/callbacks"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CallbacksMigrationSuite struct {
	testcore.FunctionalTestBase
}

func TestCallbacksMigrationSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(CallbacksMigrationSuite))
}

func (s *CallbacksMigrationSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()
	// Start with CHASM disabled by default for migration tests
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, false)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, false)
	// Enable Nexus for callbacks
	s.OverrideDynamicConfig(dynamicconfig.EnableNexus, true)
}

// TODO (seankane): This test can be removed once CHASM callbacks are the default
func (s *CallbacksMigrationSuite) TestWorkflowCallbacks_CHASM_Enabled_Mid_WF() {
	// This test verifies that when CHASM is enabled mid-workflow, callbacks still work correctly.
	// 1. Start a workflow with CHASM disabled and a callback registered
	// 2. Workflow blocks waiting for a signal
	// 3. Enable CHASM dynamically
	// 4. Send signal to unblock workflow and let it complete
	// 5. Verify callback is invoked successfully

	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	tv := testvars.New(s.T())
	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr(s.T().Name())
	workflowType := "blockingWorkflow"
	workflowID := tv.WorkflowID()

	// Setup completion handler and HTTP server
	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()

	// Start HTTP server for nexus callback completion
	callbackAddress := func() string {
		hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: ch})
		srv := httptest.NewServer(hh)
		s.T().Cleanup(func() {
			srv.Close()
		})
		return srv.URL
	}()

	// Register workflow that blocks until it receives a signal
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		// Block until we receive the "continue" signal
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	w.RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// Start workflow with callback (CHASM is disabled at this point)
	callback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress + "/callback",
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{callback},
	}

	response, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      response.RunId,
	}

	// Wait for workflow to start and reach the blocking point
	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Enable CHASM mid-workflow
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	// Unblock the workflow by sending the continue signal
	_, err = s.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "continue",
		},
	)
	s.NoError(err)

	// Wait for workflow to complete
	run := sdkClient.GetWorkflow(ctx, workflowID, "")
	var result int
	s.NoError(run.Get(ctx, &result))
	s.Equal(1, result)

	// Verify callback was invoked with successful completion
	select {
	case completion := <-ch.requestCh:
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		var callbackResult int
		s.NoError(completion.Result.Consume(&callbackResult))
		s.Equal(1, callbackResult)
		// Acknowledge the callback
		ch.requestCompleteCh <- nil
	case <-time.After(5 * time.Second):
		s.Fail("timeout waiting for callback")
	}
}

func (s *CallbacksMigrationSuite) TestWorkflowCallbacks_CHASM_Disabled_Mid_WF() {
	// This test verifies that when EnableCHASMCallbacks is disabled mid-workflow,
	// callbacks that were already created in CHASM still trigger successfully.
	//
	// 1. Enable both CHASM and CHASM callbacks
	// 2. Start a workflow with a callback registered (CHASM callback)
	// 3. Workflow blocks waiting for a signal
	// 4. Disable EnableCHASMCallbacks (but keep CHASM enabled)
	// 5. Send signal to unblock workflow and let it complete
	// 6. Verify callback is invoked successfully despite EnableCHASMCallbacks being disabled

	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	// Enable CHASM for this test
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	tv := testvars.New(s.T())
	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr(s.T().Name())
	workflowType := "blockingWorkflow"
	workflowID := tv.WorkflowID()

	// Setup completion handler and HTTP server
	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()

	// Start HTTP server for nexus callback completion
	callbackAddress := func() string {
		hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: ch})
		srv := httptest.NewServer(hh)
		s.T().Cleanup(func() {
			srv.Close()
		})
		return srv.URL
	}()

	// Register workflow that blocks until it receives a signal
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		// Block until we receive the "continue" signal
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	w.RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// Start workflow with callback (CHASM is enabled at this point)
	callback := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress + "/callback",
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{callback},
	}

	response, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      response.RunId,
	}

	// Wait for workflow to start and reach the blocking point
	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, false)

	// Unblock the workflow by sending the continue signal
	_, err = s.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "continue",
		},
	)
	s.NoError(err)

	// Wait for workflow to complete
	run := sdkClient.GetWorkflow(ctx, workflowID, "")
	var result int
	s.NoError(run.Get(ctx, &result))
	s.Equal(1, result)

	// Verify callback was invoked with successful completion
	select {
	case completion := <-ch.requestCh:
		s.Equal(nexus.OperationStateSucceeded, completion.State)
		var callbackResult int
		s.NoError(completion.Result.Consume(&callbackResult))
		s.Equal(1, callbackResult)
		// Acknowledge the callback
		ch.requestCompleteCh <- nil
	case <-time.After(5 * time.Second):
		s.Fail("timeout waiting for callback - callback should still be triggered even with EnableCHASMCallbacks disabled")
	}
}

// TODO (seankane): This test can be removed once CHASM callbacks are the default
func (s *CallbacksMigrationSuite) TestWorkflowCallbacks_MixedCallbacks() {
	// This test verifies that both HSM and CHASM callbacks work correctly together.
	// 1. Start a workflow with CHASM disabled and a callback registered (HSM callback)
	// 2. Workflow blocks waiting for a signal
	// 3. Enable CHASM dynamically
	// 4. Add another callback while CHASM is enabled (CHASM callback)
	// 5. Send signal to unblock workflow and let it complete
	// 6. Verify both callbacks (HSM and CHASM) are invoked successfully

	s.OverrideDynamicConfig(
		callbacks.AllowedAddresses,
		[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
	)

	tv := testvars.New(s.T())
	ctx := testcore.NewContext()
	sdkClient, err := client.Dial(client.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace().String(),
	})
	s.NoError(err)

	taskQueue := testcore.RandomizeStr(s.T().Name())
	workflowType := "blockingWorkflow"
	workflowID := tv.WorkflowID()

	// Setup completion handlers for both callbacks
	ch1 := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	ch2 := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch1.requestCh)
		close(ch1.requestCompleteCh)
		close(ch2.requestCh)
		close(ch2.requestCompleteCh)
	}()

	// Start HTTP servers for nexus callback completion
	callbackAddress1 := func() string {
		hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: ch1})
		srv := httptest.NewServer(hh)
		s.T().Cleanup(func() {
			srv.Close()
		})
		return srv.URL
	}()

	callbackAddress2 := func() string {
		hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: ch2})
		srv := httptest.NewServer(hh)
		s.T().Cleanup(func() {
			srv.Close()
		})
		return srv.URL
	}()

	// Register workflow that blocks until it receives a signal
	w := worker.New(sdkClient, taskQueue, worker.Options{})
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		// Block until we receive the "continue" signal
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	w.RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	// Start workflow with first callback (CHASM is disabled, so this creates an HSM callback)
	callback1 := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress1 + "/callback1",
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           s.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{callback1},
	}

	response, err := s.FrontendClient().StartWorkflowExecution(ctx, request)
	s.NoError(err)

	workflowExecution := &commonpb.WorkflowExecution{
		WorkflowId: workflowID,
		RunId:      response.RunId,
	}

	// Wait for workflow to start and reach the blocking point
	s.WaitForHistoryEvents(`
		1 WorkflowExecutionStarted
		2 WorkflowTaskScheduled
		3 WorkflowTaskStarted
		4 WorkflowTaskCompleted`,
		s.GetHistoryFunc(s.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Enable CHASM mid-workflow
	s.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	s.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	// Add a second callback using the USE_EXISTING conflict policy
	// This should create a CHASM callback since CHASM is now enabled
	callback2 := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress2 + "/callback2",
			},
		},
	}

	request2 := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:                uuid.NewString(),
		Namespace:                s.Namespace().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:                    nil,
		WorkflowRunTimeout:       durationpb.New(30 * time.Second),
		Identity:                 s.T().Name(),
		WorkflowIdConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		CompletionCallbacks:      []*commonpb.Callback{callback2},
		OnConflictOptions: &workflowpb.OnConflictOptions{
			AttachRequestId:           true,
			AttachCompletionCallbacks: true,
		},
	}

	response2, err := s.FrontendClient().StartWorkflowExecution(ctx, request2)
	s.NoError(err)
	s.False(response2.Started)
	s.Equal(workflowExecution.RunId, response2.RunId)

	// Verify DescribeWorkflow shows both callbacks (1 HSM + 1 CHASM)
	description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
	s.NoError(err)
	s.Len(description.Callbacks, 2, "should have 2 callbacks: 1 HSM + 1 CHASM")

	// Verify both callbacks are in STANDBY state (not yet triggered)
	for _, callbackInfo := range description.Callbacks {
		s.Equal(enumspb.CALLBACK_STATE_STANDBY, callbackInfo.State)
		s.Equal(int32(0), callbackInfo.Attempt)
		// Verify trigger type is WorkflowClosed
		s.NotNil(callbackInfo.Trigger)
		s.NotNil(callbackInfo.Trigger.GetWorkflowClosed())
	}

	// Unblock the workflow by sending the continue signal
	_, err = s.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         s.Namespace().String(),
			WorkflowExecution: workflowExecution,
			SignalName:        "continue",
		},
	)
	s.NoError(err)

	// Wait for workflow to complete
	run := sdkClient.GetWorkflow(ctx, workflowID, "")
	var result int
	s.NoError(run.Get(ctx, &result))
	s.Equal(1, result)

	// Verify both callbacks were invoked with successful completion
	// We need to receive both callbacks, order doesn't matter
	callbacksReceived := 0
	for callbacksReceived < 2 {
		select {
		case completion := <-ch1.requestCh:
			s.Equal(nexus.OperationStateSucceeded, completion.State)
			var callbackResult int
			s.NoError(completion.Result.Consume(&callbackResult))
			s.Equal(1, callbackResult)
			ch1.requestCompleteCh <- nil
			callbacksReceived++
		case completion := <-ch2.requestCh:
			s.Equal(nexus.OperationStateSucceeded, completion.State)
			var callbackResult int
			s.NoError(completion.Result.Consume(&callbackResult))
			s.Equal(1, callbackResult)
			ch2.requestCompleteCh <- nil
			callbacksReceived++
		case <-time.After(5 * time.Second):
			s.Failf("timeout waiting for callbacks", "only received %d of 2 callbacks", callbacksReceived)
		}
	}
	s.Equal(2, callbacksReceived)

	// Verify DescribeWorkflow shows both callbacks in SUCCEEDED state after completion
	s.EventuallyWithT(func(t *assert.CollectT) {
		description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
		require.NoError(t, err)
		require.Len(t, description.Callbacks, 2, "should still have 2 callbacks")

		// Both callbacks should now be in SUCCEEDED state
		for _, callbackInfo := range description.Callbacks {
			require.Equal(t, enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
			require.Equal(t, int32(1), callbackInfo.Attempt)
			require.Nil(t, callbackInfo.LastAttemptFailure)
			require.NotNil(t, callbackInfo.LastAttemptCompleteTime)
		}
	}, 2*time.Second, 100*time.Millisecond)
}
