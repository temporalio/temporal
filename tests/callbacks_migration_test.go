package tests

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/tests/testcore"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CallbacksMigrationSuite struct {
	parallelsuite.Suite[*CallbacksMigrationSuite]
}

func TestCallbacksMigrationSuite(t *testing.T) {
	parallelsuite.Run(t, &CallbacksMigrationSuite{})
}

func (s *CallbacksMigrationSuite) newTestEnv() *testcore.TestEnv {
	return testcore.NewEnv(
		s.T(),
		testcore.WithDynamicConfig(
			callback.AllowedAddresses,
			[]any{map[string]any{"Pattern": "*", "AllowInsecure": true}},
		),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, false),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, false),
	)
}

func (s *CallbacksMigrationSuite) runNexusCompletionHTTPServer(h *completionHandler) string {
	hh := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{Handler: h})
	srv := httptest.NewServer(hh)
	s.T().Cleanup(func() {
		srv.Close()
	})
	return srv.URL
}

// TODO (seankane): This test can be removed once CHASM callbacks are the default
func (s *CallbacksMigrationSuite) TestWorkflowCallbacks_CHASM_Enabled_Mid_WF() {
	// This test verifies that when CHASM is enabled mid-workflow, callbacks still work correctly.
	// 1. Start a workflow with CHASM disabled and a callback registered
	// 2. Workflow blocks waiting for a signal
	// 3. Enable CHASM dynamically
	// 4. Send signal to unblock workflow and let it complete
	// 5. Verify callback is invoked successfully

	env := s.newTestEnv()

	ctx := s.Context()
	sdkClient := env.SdkClient()

	workflowType := "blockingWorkflow"
	workflowID := env.Tv().WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(ch)

	// Register workflow that blocks until it receives a signal
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	// Start workflow with callback (CHASM is disabled at this point)
	cb := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress + "/callback",
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{cb},
	}

	response, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
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
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Enable CHASM mid-workflow
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	// Unblock the workflow by sending the continue signal
	_, err = env.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
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

	env := s.newTestEnv()

	// Enable CHASM for this test
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

	ctx := s.Context()
	sdkClient := env.SdkClient()

	workflowType := "blockingWorkflow"
	workflowID := env.Tv().WorkflowID()

	ch := &completionHandler{
		requestCh:         make(chan *nexusrpc.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	defer func() {
		close(ch.requestCh)
		close(ch.requestCompleteCh)
	}()
	callbackAddress := s.runNexusCompletionHTTPServer(ch)

	// Register workflow that blocks until it receives a signal
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

	// Start workflow with callback (CHASM is enabled at this point)
	cb := &commonpb.Callback{
		Variant: &commonpb.Callback_Nexus_{
			Nexus: &commonpb.Callback_Nexus{
				Url: callbackAddress + "/callback",
			},
		},
	}

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{cb},
	}

	response, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
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
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, false)

	// Unblock the workflow by sending the continue signal
	_, err = env.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
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

	env := s.newTestEnv()

	ctx := s.Context()
	sdkClient := env.SdkClient()

	workflowType := "blockingWorkflow"
	workflowID := env.Tv().WorkflowID()

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

	callbackAddress1 := s.runNexusCompletionHTTPServer(ch1)
	callbackAddress2 := s.runNexusCompletionHTTPServer(ch2)

	// Register workflow that blocks until it receives a signal
	blockingWorkflow := func(ctx workflow.Context) (int, error) {
		workflow.GetSignalChannel(ctx, "continue").Receive(ctx, nil)
		return 1, nil
	}
	env.SdkWorker().RegisterWorkflowWithOptions(blockingWorkflow, workflow.RegisterOptions{Name: workflowType})

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
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(30 * time.Second),
		Identity:            s.T().Name(),
		CompletionCallbacks: []*commonpb.Callback{callback1},
	}

	response, err := env.FrontendClient().StartWorkflowExecution(ctx, request)
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
		env.GetHistoryFunc(env.Namespace().String(), workflowExecution),
		5*time.Second,
		10*time.Millisecond)

	// Enable CHASM mid-workflow
	env.OverrideDynamicConfig(dynamicconfig.EnableChasm, true)
	env.OverrideDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true)

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
		Namespace:                env.Namespace().String(),
		WorkflowId:               workflowID,
		WorkflowType:             &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:                &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
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

	response2, err := env.FrontendClient().StartWorkflowExecution(ctx, request2)
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
		s.NotNil(callbackInfo.Trigger)
		s.NotNil(callbackInfo.Trigger.GetWorkflowClosed())
	}

	// Unblock the workflow by sending the continue signal
	_, err = env.FrontendClient().SignalWorkflowExecution(
		ctx,
		&workflowservice.SignalWorkflowExecutionRequest{
			Namespace:         env.Namespace().String(),
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
	s.Await(func(suite *CallbacksMigrationSuite) {
		description, err := sdkClient.DescribeWorkflowExecution(ctx, workflowID, "")
		suite.NoError(err)
		suite.Len(description.Callbacks, 2, "should still have 2 callbacks")

		// Both callbacks should now be in SUCCEEDED state
		for _, callbackInfo := range description.Callbacks {
			suite.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
			suite.Equal(int32(1), callbackInfo.Attempt)
			suite.Nil(callbackInfo.LastAttemptFailure)
			suite.NotNil(callbackInfo.LastAttemptCompleteTime)
		}
	}, await.WithTimeout(2*time.Second), await.WithMinPollInterval(100*time.Millisecond), await.WithMaxPollInterval(100*time.Millisecond))
}
