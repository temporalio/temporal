package tests

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	notificationpb "go.temporal.io/api/notificationservice/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
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
		// TODO: This needs to be switched over to using SANOs. Because Workflows won't support it.
		// testcore.WithDynamicConfig(callback.EnabledWorkflowCallbackKinds, []string{"nexus", "worker"}),
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
