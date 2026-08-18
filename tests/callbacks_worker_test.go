package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	callbackpb "go.temporal.io/api/callback/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/notificationservice/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/temporalnexus"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/chasm/lib/callback"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/tests/testcore"
)

// WorkerCallbacksSuite covers Worker-variant completion callbacks, which deliver an execution's
// outcome to a Nexus service on a worker polling within the same namespace. Worker callbacks are
// only implemented by the CHASM callback library, so these tests always enable it.
type WorkerCallbacksSuite struct {
	parallelsuite.Suite[*WorkerCallbacksSuite]
}

func TestWorkerCallbacksSuite(t *testing.T) {
	parallelsuite.Run(t, &WorkerCallbacksSuite{})
}

// TestWorkflowCompletionDeliveredToWorker starts a workflow with a Worker-variant completion
// callback and asserts the completion reaches the handler the callback names.
func (s *WorkerCallbacksSuite) TestWorkflowCompletionDeliveredToWorker() {
	env := testcore.NewEnv(s.T(),
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(callback.EnabledWorkflowCallbackKinds, []string{"nexus", "worker"}),
	)

	ctx := s.Context()
	t := s.T()

	// Buffered so the handler never blocks the worker, and a redelivery cannot wedge it either.
	completions := make(chan *notificationpb.OnCompleteRequest, 1)
	service := nexus.NewService("completion-service")
	operation := nexus.NewSyncOperation(
		"on-complete",
		func(_ context.Context, req *notificationpb.OnCompleteRequest, _ nexus.StartOperationOptions) (*notificationpb.OnCompleteResponse, error) {
			select {
			case completions <- req:
			default:
			}
			return &notificationpb.OnCompleteResponse{}, nil
		},
	)
	s.NoError(service.Register(operation))

	// The handler polls its own task queue, so delivery has to be routed by the callback rather
	// than by the workflow's task queue.
	handlerTaskQueue := testcore.RandomizeStr(t.Name())
	handlerWorker := sdkworker.New(env.SdkClient(), handlerTaskQueue, sdkworker.Options{})
	handlerWorker.RegisterNexusService(service)
	s.NoError(handlerWorker.Start())
	defer handlerWorker.Stop()

	// A workflow that returns no value at all, only a nil error.
	workflowType := "worker-callback-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(workflow.Context) error { return nil },
		workflow.RegisterOptions{Name: workflowType},
	)
	failingWorkflowType := "worker-callback-failing-workflow"
	env.SdkWorker().RegisterWorkflowWithOptions(
		func(workflow.Context) error {
			return temporal.NewNonRetryableApplicationError("workflow failed on purpose", "TestError", nil)
		},
		workflow.RegisterOptions{Name: failingWorkflowType},
	)

	sourceContext := payload.EncodeString("source-context")
	workerCallback := &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: handlerTaskQueue,
				Service:       service.Name,
				Operation:     operation.Name(),
				SourceContext: sourceContext,
			},
		},
	}

	workflowID := env.Tv().WorkflowID()
	_, err := env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          workflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            t.Name(),
		CompletionCallbacks: []*commonpb.Callback{workerCallback},
	})
	s.NoError(err)
	s.NoError(env.SdkClient().GetWorkflow(ctx, workflowID, "").Get(ctx, nil))

	var completion *notificationpb.OnCompleteRequest
	select {
	case completion = <-completions:
	case <-ctx.Done():
		s.FailNow("timed out waiting for the worker callback")
	}

	// The workflow succeeded without a result, which is delivered as the binary/null encoding of
	// "no value" rather than as a failure or an empty payload.
	s.Require().IsType(&notificationpb.OnCompleteRequest_Success{}, completion.GetResult())
	s.Nil(completion.GetFailure())
	nilPayload, err := payload.Encode(nil)
	s.NoError(err)
	protorequire.ProtoEqual(t, nilPayload, completion.GetSuccess())

	// The context the callback was registered with is carried to the handler untouched.
	protorequire.ProtoEqual(t, sourceContext, completion.GetSourceContext())

	// The handler answered with a successful operation, so the callback is done rather than
	// backing off for another attempt. The state is recorded after the handler responds, so it
	// may not have landed yet.
	await.Require(ctx, t, func(c *await.T) {
		description, descErr := env.SdkClient().DescribeWorkflowExecution(c.Context(), workflowID, "")
		require.NoError(c, descErr)
		require.Len(c, description.GetCallbacks(), 1)
		require.Equal(c, enumspb.CALLBACK_STATE_SUCCEEDED, description.GetCallbacks()[0].GetState())
		protorequire.ProtoEqual(c, workerCallback, description.GetCallbacks()[0].GetCallback())
	}, 10*time.Second, 100*time.Millisecond)

	// A failed workflow delivers the failure to the same handler, in place of a result.
	failedWorkflowID := env.Tv().Any().String()
	_, err = env.FrontendClient().StartWorkflowExecution(ctx, &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.NewString(),
		Namespace:           env.Namespace().String(),
		WorkflowId:          failedWorkflowID,
		WorkflowType:        &commonpb.WorkflowType{Name: failingWorkflowType},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: env.WorkerTaskQueue(), Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            t.Name(),
		CompletionCallbacks: []*commonpb.Callback{workerCallback},
	})
	s.NoError(err)
	s.Error(env.SdkClient().GetWorkflow(ctx, failedWorkflowID, "").Get(ctx, nil))

	select {
	case completion = <-completions:
	case <-ctx.Done():
		s.FailNow("timed out waiting for the worker callback of the failed workflow")
	}

	s.Require().IsType(&notificationpb.OnCompleteRequest_Failure{}, completion.GetResult())
	s.Nil(completion.GetSuccess())
	// The operation error wrapping the workflow failure is unwrapped, so the handler is handed the
	// underlying cause.
	s.Equal("workflow failed on purpose", completion.GetFailure().GetMessage())
	s.Equal("TestError", completion.GetFailure().GetApplicationFailureInfo().GetType())
}

// workerCallbacksService is the test scaffold for testing worker callback deliveries.
// Calling NewWorkerCallbacksService(...) will spawn a new Temporal worker in the
// test environment's namespace, registering a randomized Nexus service and operation
// that is capable of receiving worker callbacks.
type workerCallbacksService struct {
	TaskQueue     string
	ServiceName   string
	OperationName string

	invocationCh chan *notificationservice.OnCompleteRequest
	// Channel for writing invocation results. A nil value will return an
	// empty notificationservice.OnCompelteResponse{}.
	invocationResultCh chan error
	doneCh             chan struct{}
}

func NewWorkerCallbacksService(t *testing.T, env *testcore.TestEnv) (*workerCallbacksService, error) {
	taskQueue := "wc-taskqueue-" + uuid.NewString()
	svcName := "workercallbacks-svc-" + uuid.NewString()
	opName := "OnCompleteHandler"
	workflowType := "workercallbacks-handler-workflow"

	// Buffered so the server can deliver several completions (or retries) before the test drains them.
	wcl := &workerCallbacksService{
		TaskQueue:     taskQueue,
		ServiceName:   svcName,
		OperationName: opName,

		invocationCh:       make(chan *notificationservice.OnCompleteRequest, 4),
		invocationResultCh: make(chan error, 4),
		doneCh:             make(chan struct{}),
	}

	// The Workflow that is backing the async Nexus operation. It has to be registered on the worker
	// below under the same name the operation resolves it to, otherwise the operation starts a
	// workflow the worker cannot execute and the handler is never reached.
	handlerWorkflow := func(ctx workflow.Context, input *notificationservice.OnCompleteRequest) (*notificationservice.OnCompleteResponse, error) {
		return wcl.handle(input)
	}

	nexusSvc := nexus.NewService(svcName)
	nexusOp := temporalnexus.NewWorkflowRunOperation(
		opName,
		handlerWorkflow,
		func(
			ctx context.Context,
			input *notificationservice.OnCompleteRequest,
			startOpts nexus.StartOperationOptions) (client.StartWorkflowOptions, error) {
			resp := client.StartWorkflowOptions{
				ID: uuid.NewString(),
			}
			return resp, nil
		})

	if err := nexusSvc.Register(nexusOp); err != nil {
		return nil, fmt.Errorf("registering Nexus operation: %w", err)
	}

	worker := sdkworker.New(env.SdkClient(), taskQueue, sdkworker.Options{})
	// Registering the same func value the operation was built from is what ties the two together:
	// the operation resolves the workflow type through the worker's registry, so it starts
	// workflowType rather than the closure's reflected name ("func1").
	worker.RegisterWorkflowWithOptions(handlerWorkflow, workflow.RegisterOptions{Name: workflowType})
	worker.RegisterNexusService(nexusSvc)

	// Start the worker. Completion handlers can now be delivered successfully.
	if err := worker.Start(); err != nil {
		return nil, fmt.Errorf("starting worker: %w", err)
	}

	t.Cleanup(func() {
		close(wcl.doneCh)
		worker.Stop()
	})
	return wcl, nil
}

// handle is the implementation of the actual completion handler.
func (wcl *workerCallbacksService) handle(
	input *notificationservice.OnCompleteRequest,
) (*notificationservice.OnCompleteResponse, error) {
	// Push the request to the invocations channel.
	select {
	case wcl.invocationCh <- input:
	case <-wcl.doneCh:
		return nil, nil
	}

	// Pull from the invocation results channel.
	select {
	case err := <-wcl.invocationResultCh:
		if err == nil {
			return &notificationservice.OnCompleteResponse{}, nil
		}
		return nil, err
	case <-wcl.doneCh:
		return nil, nil
	}
}

// WorkerCallback returns a Worker-variant callback to be routed to the
// worker callbacks service.
func (wcl *workerCallbacksService) WorkerCallback() *commonpb.Callback {
	return &commonpb.Callback{
		Variant: &commonpb.Callback_Worker_{
			Worker: &commonpb.Callback_Worker{
				TaskQueueName: wcl.TaskQueue,
				Service:       wcl.ServiceName,
				Operation:     wcl.OperationName,
				// TODO(chrsmith): Confirm we have tests verifying the source context payload
				// is wired through correctly.
				SourceContext: nil,
			},
		},
	}
}

func (s *WorkerCallbacksSuite) TestSANOCompletionDeliveredToWorker() {
	env := newNexusTestEnv(s.T(), true,
		testcore.WithDynamicConfig(dynamicconfig.EnableChasm, true),
		testcore.WithDynamicConfig(dynamicconfig.EnableCHASMCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.Enabled, true),
		testcore.WithDynamicConfig(nexusoperation.EnableCallbacks, true),
		testcore.WithDynamicConfig(nexusoperation.EnabledCallbackKinds, []string{"worker"}),
	)

	ctx, cancel := context.WithTimeout(s.Context(), 20*time.Second)
	defer cancel()

	t := s.T()

	// Spin up a new Temporal worker powering a Nexus service with completion handler.
	// This what we will inspect to confirm that worker callbacks are being delivered.
	wcSvc, err := NewWorkerCallbacksService(t, env.TestEnv)
	s.NoError(err)

	// Register a Nexus service and operation. This is what the SANO execution will invoke.
	// It's result will be supplied to the worker callback.
	const sanoResultText = "sano-op-result-xxx"
	alwaysSuccessEndpointName := env.createSyncSuccessEndpoint(s.Context(), s.T(), sanoResultText)

	// Invoke the SANO, with the registered completion callback.
	operationID := "sano-execution-" + uuid.NewString()
	startResp, err := env.startNexusOperation(ctx, &workflowservice.StartNexusOperationExecutionRequest{
		OperationId:         operationID,
		Endpoint:            alwaysSuccessEndpointName,
		CompletionCallbacks: []*commonpb.Callback{wcSvc.WorkerCallback()},
	})
	s.NoError(err)
	s.True(startResp.GetStarted())

	// Wait for the SANO to complete.
	s.Await(func(s *WorkerCallbacksSuite) {
		describeResp := env.describeNexusOperation(ctx, s.T(), operationID)
		status := describeResp.GetInfo().GetStatus()
		s.Equal(enumspb.NEXUS_OPERATION_EXECUTION_STATUS_COMPLETED, status)
	}, 5*time.Second, 100*time.Millisecond)

	// The SANO has completed, we now expect the worker callback to be delivered.
	select {
	case <-ctx.Done():
		s.FailNow("timed out waiting for the completion callback")
	case gotInput := <-wcSvc.invocationCh:
		s.Nil(gotInput.GetFailure())
		s.NotNil(gotInput.GetSuccess())
		gotPayload := gotInput.GetSuccess()

		// Confirm the payload delivered contained the right payload. The handler returned a Go
		// string, so the result travels as its JSON encoding.
		s.JSONEq(`"`+sanoResultText+`"`, string(gotPayload.GetData()))

		// Have the worker callback report a successful delivery.
		wcSvc.invocationResultCh <- nil
	}

	// Wait for the SANO's completion callback to be resolved.
	cbInfo := s.awaitCallbackInfo(env, operationID, enumspb.CALLBACK_STATE_SUCCEEDED)
	s.NotNil(cbInfo.GetSuccess())
}

// HACK: This was cribbed from nexus_standalone_callbacks_test.go, and should go into nexus_test_base.go
func (s *WorkerCallbacksSuite) awaitCallbackInfo(
	env *NexusTestEnv,
	operationID string,
	wantState enumspb.CallbackState,
) *callbackpb.CallbackInfo {
	s.T().Helper()

	var cbInfo *callbackpb.CallbackInfo
	await.Require(s.Context(), s.T(), func(c *await.T) {
		cbs := env.describeNexusOperation(c.Context(), c, operationID).GetCompletionCallbacks()
		require.Len(c, cbs, 1)
		cbInfo = cbs[0].GetInfo()
		require.NotNil(c, cbInfo)
		require.Equal(c, wantState, cbInfo.GetState())
	}, 10*time.Second, 100*time.Millisecond)
	return cbInfo
}
